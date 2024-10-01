#!/usr/bin/env python3

import argparse
import json
import os
import subprocess
import threading
import time
import sqlite3
import shutil
import logging
import queue
import concurrent.futures
import shelve
import time
from collections import defaultdict

CONFIG_FILE_PATH = "/etc/drive-manager/config.json"
IO_THREADS = 4
TIERING_CHECK_INTERVAL = 7200  # 2 hours in seconds
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DriveManager:

    # Define paths
    MOUNT_PATH = "/mnt/physical"
    MERGERFS_MOUNT_PATH = "/mnt/merged"

    NEW_DRIVE_MOUNTED = False
    LSBLK_DISCOVER_CMD = [
        "lsblk",
        "--all",
        "-po",
        "ALIGNMENT,DISC-ALN,DAX,DISC-GRAN,DISC-MAX,DISC-ZERO,FSAVAIL,FSROOTS,FSSIZE,FSTYPE,FSUSED,FSUSE%,FSVER,GROUP,HCTL,HOTPLUG,KNAME,LABEL,LOG-SEC,MAJ:MIN,MIN-IO,MODE,MODEL,NAME,OPT-IO,OWNER,PARTFLAGS,PARTLABEL,PARTTYPE,PARTTYPENAME,PARTUUID,PATH,PHY-SEC,PKNAME,PTTYPE,PTUUID,RA,RAND,REV,RM,RO,ROTA,RQ-SIZE,SCHED,SERIAL,SIZE,START,STATE,SUBSYSTEMS,MOUNTPOINT,MOUNTPOINTS,TRAN,TYPE,UUID,VENDOR,WSAME,WWN,ZONED,ZONE-SZ,ZONE-WGRAN,ZONE-APP,ZONE-NR,ZONE-OMAX,ZONE-AMAX",
        "--json",
    ]

    def __init__(self, args):
        self.args = args
        self.config = self._read_config()
        self.new_drive_mounted = False
        self.tiering_manager = TieringManager(self)

    # Run commands
    def run_command(self, cmd, as_str=False):
        cmd_str = " ".join(cmd)
        if self.args.dryrun:
            logging.info(" ".join(["DRYRUN:", cmd_str]))
            return None
        else:
            logging.info(cmd_str)
            if as_str:
                return subprocess.getoutput(cmd_str)
            return subprocess.run(cmd)

    # Function to get access times of files in a directory
    def get_atime(directory):
        atime_dict = defaultdict(int)
        for root, _, files in os.walk(directory):
            for file in files:
                filepath = os.path.join(root, file)
                atime = os.path.getatime(filepath)
                atime_dict[filepath] = atime
        return atime_dict

    def rsync(self, src, dest):
        rsync_command = [
            "rsync",
            "-axqHAXWES",
            "--preallocate",
            "--remove-source-files",
            src,
            dest,
        ]
        if self.args.dryrun:
            logging.info(f"[DRY RUN] Would run rsync command: {' '.join(rsync_command)}")
            return True
        logging.info(f"Running rsync command: {' '.join(rsync_command)}")
        try:
            subprocess.run(rsync_command, check=True)
            return True
        except subprocess.CalledProcessError as e:
            logging.error(f"Rsync command failed: {e}")
            return False

    def sort_block_device(self, block_device):
        # Define the sorting order based on block_class
        class_order = {"nvme": 0, "ssd": 1, "hdd": 2}

        # Sort by block_class within the same tier
        block_class = block_device["block_class"]

        return class_order[block_class]

    # setup mergerfs mounts
    def setup_mergerfs(self, active_block_devices):
        mergerfs_opts = [
            "allow_other",
            "nonempty",
            "lazy-umount-mountpoint=true",
            "moveonenospc=true",
            "cache.files=auto-full",
            "parallel-direct-writes=true",
            "cache.writeback=true",
            "cache.statfs=true",
            "cache.symlinks=true",
            "cache.readdir=true",
            "posix_acl=false",
            "async_read=false",
            "dropcacheonclose=true",
            # 'category.create=ff'
        ]

        tier_devices = {
            "hot": active_block_devices,
            "warm": [
                device
                for device in active_block_devices
                if device["block_class"] != "nvme"
            ],
            "cold": [
                device
                for device in active_block_devices
                if device["block_class"] == "hdd"
            ],
        }
        tier_devices = {
            key: sorted(value, key=self.sort_block_device)
            for key, value in tier_devices.items()
        }

        for tier, devices in tier_devices.items():
            logging.info(f"{tier.title()} Devices: {[device['serial'] for device in devices]}")

        # extract active drive class mount
        tier_globs = {
            tier: ":".join(device["children"][0]["mountpoint"] for device in devices)
            for tier, devices in tier_devices.items()
        }

        # logging.info(json.dumps(drive_classes, indent=4))
        for tier, glob in tier_globs.items():
            mount_point = os.path.join(self.MERGERFS_MOUNT_PATH, tier)
            os.makedirs(
                mount_point, exist_ok=True
            )  # Create mount point if it doesn't exist
            if tier == "cold":
                mergerfs_cmd = [
                    "mergerfs",
                    "-o",
                    ",".join(mergerfs_opts + ["category.create=mfs"]),
                    glob,
                    mount_point,
                ]
            else:
                mergerfs_cmd = [
                    "mergerfs",
                    "-o",
                    ",".join(mergerfs_opts + ["category.create=ff"]),
                    glob,
                    mount_point,
                ]
            self.run_command(mergerfs_cmd)
            # After setting up MergerFS, start the tiering manager
            self.tiering_manager.start_background_process()

    # Function to mount drive
    def mount_drive(self, block_device):
        mount_point = os.path.join(
            self.MOUNT_PATH, block_device["block_class"], block_device["serial"]
        )
        os.makedirs(
            mount_point, exist_ok=True
        )  # Create mount point if it doesn't exist
        part_path = block_device["children"][0]["path"]
        part_mount_point = block_device["children"][0]["mountpoint"]
        # only mount if not already mounted in expected location
        if part_mount_point != mount_point:
            mount_cmd = ["mount", part_path, mount_point]
            self.run_command(mount_cmd)
            self.new_drive_mounted = True

        # get new partition info
        return self.update_block_device(block_device)

    # Function to format drive to specified filesystem
    def format_drive(self, block_device):
        filesystem = self.config.get("filesystem")
        if "children" in block_device.keys():
            for partition in block_device["children"]:
                # attempt to umount all partitions
                umount_cmd = ["umount", "-l", partition["path"]]
                self.run_command(umount_cmd)

        # wipe other file systems
        wipefs_cmd = ["wipefs", "--all", "--force", block_device["path"]]
        self.run_command(wipefs_cmd)
        # create new blank partition
        create_part_cmd = [
            "parted",
            "-a",
            "optimal",
            block_device["path"],
            "mklabel",
            "gpt",
            "mkpart",
            "primary",
            filesystem,
            "0%",
            "100%",
        ]
        self.run_command(create_part_cmd)
        # get new partition info
        self.update_block_device(block_device)
        parts = block_device["children"]
        # make specified file system
        mkfs_cmd = ["yes", "|", "mkfs", "-t", filesystem.lower(), parts[0]["path"]]
        self.run_command(mkfs_cmd, as_str=True)
        # mount partition
        return self.mount_drive(block_device)

    # Read config file
    def _read_config(self):
        with open(self.args.config, "r") as file:
            return json.load(file)

    # Deprecated
    # Function to get partition information of a drive
    def _get_partitions(self, device_name, drive_path):
        output = subprocess.check_output(
            ["lsblk", "-no", "name,path,fstype,mountpoint", drive_path, "--raw"]
        ).decode("utf-8")
        part_data = [line.split() for line in output.split("\n") if line]

        # build partition data info
        drive_partitions = []
        for part in part_data:
            if part[0] != device_name:
                name = part[0]
                path = part[1]
                fstype = ""
                if len(part) >= 3:
                    fstype = part[2]
                mountpoint = ""
                if len(part) >= 4:
                    mountpoint = part[3]

                drive_partitions.append(
                    {
                        "name": name,
                        "path": path,
                        "fstype": fstype,
                        "mountpoint": mountpoint,
                    }
                )

        return drive_partitions

    # Function to update a single block device
    def update_block_device(self, block_device):
        output = subprocess.check_output(
            self.LSBLK_DISCOVER_CMD + [block_device["path"]]
        ).decode("utf-8")
        block_device = json.loads(output)["blockdevices"][0]
        # logging.info(json.dumps(block_device, indent=2))
        self._classify_block_class(block_device)
        # logging.info(json.dumps(block_device, indent=2))
        return block_device

    # Function to get the block drive class
    def _classify_block_class(self, block_device):
        if not block_device["rota"]:
            if block_device["tran"] == "nvme":
                block_class = "nvme"
                tier = "hot"
            else:
                block_class = "ssd"
                tier = "warm"
        else:
            block_class = "hdd"
            tier = "cold"

        block_device["tier"] = tier
        block_device["block_class"] = block_class

    # Function to get list of drives
    def get_block_devices(self):
        output = subprocess.check_output(
            ["lsblk", "-dno", "path,type", "--json"]
        ).decode("utf-8")
        drives_dict = json.loads(output)
        block_devices = []

        for block_device in drives_dict["blockdevices"]:
            # skip all non block devices
            if block_device["type"] != "disk":
                continue
            block_devices.append(self.update_block_device(block_device))

        return block_devices

class TieringManager:

    def __init__(self, drive_manager):
        self.drive_manager = drive_manager
        self.db_path = "/etc/drive-manager/file_metadata.db"
        self.db = shelve.open(self.db_path)
        self.move_queue = queue.Queue()
        self.retry_queue = queue.Queue()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=IO_THREADS)

    def setup_database(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS file_metadata
                     (file_path TEXT PRIMARY KEY, last_access_time INTEGER, 
                      access_count INTEGER, last_tier_move INTEGER, file_size INTEGER)''')
        conn.commit()
        conn.close()

    def start_background_process(self):
        threading.Thread(target=self.tiering_check_loop, daemon=True).start()
        threading.Thread(target=self.file_mover_loop, daemon=True).start()
        threading.Thread(target=self.retry_loop, daemon=True).start()
        threading.Thread(target=self.maintenance_loop, daemon=True).start()

    def tiering_check_loop(self):
        while True:
            self.perform_tiering_check()
            time.sleep(TIERING_CHECK_INTERVAL)

    def file_mover_loop(self):
        while True:
            try:
                file_info = self.move_queue.get(timeout=1)
                self.executor.submit(self.move_file, file_info)
            except queue.Empty:
                continue

    def retry_loop(self):
        while True:
            try:
                file_info = self.retry_queue.get(timeout=60)
                if file_info['retries'] < 3:
                    file_info['retries'] += 1
                    self.move_queue.put(file_info)
                else:
                    logging.error(f"Failed to move file after 3 retries: {file_info['src']}")
            except queue.Empty:
                continue

    def perform_tiering_check(self):
        logging.info("Starting tiering check")
        self.update_file_metadata()
        self.check_tier_capacities()
        self.move_files_based_on_rules()
        logging.info("Tiering check completed")

    def update_file_metadata(self):
        for tier in ["hot", "warm", "cold"]:
            tier_path = os.path.join(self.drive_manager.MERGERFS_MOUNT_PATH, tier)
            for root, _, files in os.walk(tier_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, self.drive_manager.MERGERFS_MOUNT_PATH)
                    atime = os.path.getatime(file_path)
                    size = os.path.getsize(file_path)
                    
                    if relative_path in self.db:
                        self.db[relative_path].update({
                            'last_access_time': atime,
                            'access_count': self.db[relative_path]['access_count'] + 1,
                            'file_size': size,
                            'tier': tier
                        })
                    else:
                        self.db[relative_path] = {
                            'last_access_time': atime,
                            'access_count': 1,
                            'file_size': size,
                            'tier': tier
                        }
        self.db.sync()

    def check_tier_capacities(self):
        for tier in ["hot", "warm", "cold"]:
            tier_path = os.path.join(self.drive_manager.MERGERFS_MOUNT_PATH, tier)
            total, used, free = shutil.disk_usage(tier_path)
            usage_percent = (used / total) * 100
            if usage_percent > self.drive_manager.config.get("tier_capacity_threshold", 85):
                self.move_files_down(tier)

    def move_files_down(self, source_tier):
        target_tier = "warm" if source_tier == "hot" else "cold"
        files_to_move = sorted(
            [
                (k, v) for k, v in self.db.items()
                if v['tier'] == source_tier
            ],
            key=lambda x: x[1]['last_access_time']
        )[:10]
        
        for file_path, _ in files_to_move:
            self.queue_file_move(file_path, source_tier, target_tier)


    def move_files_based_on_rules(self):
        access_time_threshold = int(time.time()) - self.drive_manager.config.get("access_time_threshold", 28800)
        access_count_threshold = self.drive_manager.config.get("access_count_threshold", 3)
        
        files_to_move_up = [
            (k, v) for k, v in self.db.items()
            if v['access_count'] >= access_count_threshold and
            v['last_access_time'] > access_time_threshold and
            v['tier'] != "hot"
        ]
        
        for file_path, file_info in files_to_move_up:
            self.queue_file_move(file_path, file_info['tier'], "hot")


    def queue_file_move(self, file_path, source_tier, target_tier):
        self.move_queue.put({
            'src': file_path,
            'source_tier': source_tier,
            'target_tier': target_tier,
            'retries': 0
        })

    def move_file(self, file_info):
        src = file_info['src']
        source_tier = file_info['source_tier']
        target_tier = file_info['target_tier']

        source_path = os.path.join(self.drive_manager.MERGERFS_MOUNT_PATH, source_tier)
        target_path = os.path.join(self.drive_manager.MERGERFS_MOUNT_PATH, target_tier)
        relative_path = os.path.relpath(src, source_path)
        dest = os.path.join(target_path, relative_path)
    
        os.makedirs(os.path.dirname(dest), exist_ok=True)
    
        if self.drive_manager.run_command(["rsync", "-axqHAXWES", "--preallocate", "--remove-source-files", src, dest]):
            logging.info(f"Moved file from {src} to {dest}")
            self.db[relative_path]['tier'] = target_tier
            self.db[relative_path]['last_tier_move'] = int(time.time())
            self.db.sync()
        else:
            logging.warning(f"Failed to move file {src}. Queueing for retry.")
            self.retry_queue.put(file_info)

    def validate_and_update_database(self):
        logging.info("Starting database validation and update")
        for tier in ["hot", "warm", "cold"]:
            tier_path = os.path.join(self.drive_manager.MERGERFS_MOUNT_PATH, tier)
            for root, _, files in os.walk(tier_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, self.drive_manager.MERGERFS_MOUNT_PATH)
                    
                    if relative_path in self.db:
                        if self.db[relative_path]['tier'] != tier:
                            logging.info(f"Updating tier for {relative_path} from {self.db[relative_path]['tier']} to {tier}")
                            self.db[relative_path]['tier'] = tier
                    else:
                        logging.info(f"Adding new file to database: {relative_path}")
                        self.db[relative_path] = {
                            'tier': tier,
                            'last_access_time': os.path.getatime(file_path),
                            'access_count': 1,
                            'file_size': os.path.getsize(file_path)
                        }
        
        # Remove entries for files that no longer exist
        to_remove = []
        for relative_path in self.db:
            full_path = os.path.join(self.drive_manager.MERGERFS_MOUNT_PATH, relative_path)
            if not os.path.exists(full_path):
                to_remove.append(relative_path)
                logging.info(f"Removing non-existent file from database: {relative_path}")
        
        for relative_path in to_remove:
            del self.db[relative_path]
        
        self.db.sync()
        logging.info("Database validation and update completed")

    def maintenance_loop(self):
        while True:
            try:
                self.validate_and_update_database()
                time.sleep(86400)  # Run once a day
            except Exception as e:
                logging.error(f"Error in maintenance loop: {e}")
                time.sleep(3600)  # Wait an hour before trying again if there's an error

        
# Main function
def main():
    # Create ArgumentParser object
    parser = argparse.ArgumentParser(
        description="Utility to format, mount and pool drives together."
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="Run in dryrun mode. Does not format or mount drives, just prints actions",
    )
    parser.add_argument(
        "-c",
        "--config",
        default=CONFIG_FILE_PATH,
        help="Override config file. Default: " + CONFIG_FILE_PATH,
    )
    parser.add_argument(
        "-t",
        "--threads",
        default=IO_THREADS,
        help="Number of Rsync threads to use when performing tier operations. Default: "
        + str(IO_THREADS),
    )

    args = parser.parse_args()
    drive_manager = DriveManager(args)

    config = drive_manager.config
    exclude_drives = config.get("exclude_drives", [])
    filesystem = config.get("filesystem", [])
    logging.info(f"Excluding drives: {exclude_drives}")

    # Scan drives
    block_devices = drive_manager.get_block_devices()
    active_drives = []
    # logging.info(json.dumps(block_devices, indent=2))

    for block_device in block_devices:
        # Check if drive is partitioned and contains correct filesystem
        serial = block_device["serial"]
        path = block_device["path"]
        block_class = block_device["block_class"]
        partitions = block_device.get("children")

        # Filter out exclusions
        if serial in exclude_drives:
            logging.info(f"{path} {serial} to be excluded")
        elif (
            partitions is not None
            and len(partitions) == 1
            and partitions[0]["fstype"] == filesystem
        ):
            logging.info(f"{path} {serial} to be mounted as {block_class}")
            active_drives.append(drive_manager.mount_drive(block_device))
        else:
            logging.info(f"{path} {serial} to be formatted as {block_class}")
            active_drives.append(drive_manager.format_drive(block_device))

    # if drive_manager.NEW_DRIVE_MOUNTED or args.dryrun:
    #     logging.info('drive mounts adjusted, attempting to remount mergerfs')
    #     drive_manager.setup_mergerfs(active_drives)
    drive_manager.setup_mergerfs(active_drives)
    drive_manager.tiering_manager.start_background_process()

    # Keep the main thread running
    try:
        while True:
            time.sleep(3600)  # Sleep for an hour
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
    finally:
        # Perform any cleanup if necessary
        logging.info("Drive manager shutting down.")


if __name__ == "__main__":
    main()


# get all mount disk usage and add as json:
# $ df -hP | awk 'BEGIN {printf"{\"discarray\":["}{if($1=="Filesystem")next;if(a)printf",";printf"{\"mount\":\""$6"\",\"size\":\""$2"\",\"used\":\""$3"\",\"avail\":\""$4"\",\"use%\":\""$5"\"}";a++;}END{print"]}";}'
