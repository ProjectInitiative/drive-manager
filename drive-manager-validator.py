#!/usr/bin/env python3
import os
import random
import time
import shutil
import threading
import json
import logging
from datetime import datetime, timedelta
import shelve

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TIERING_CHECK_INTERVAL = 7200  # 2 hours in seconds

class TieringValidator:
    def __init__(self, config_path):
        self.config = self._read_config(config_path)
        self.db_path = self.config.get('db_path', '/etc/drive-manager/file_metadata.db')
        self.mergerfs_mount = self.config.get('mergerfs_mount', '/mnt/merged')
        self.tiers = ['hot', 'warm', 'cold']
        self.test_file_size = self.config.get('test_file_size', 1024 * 1024)  # 1 MB default
        self.test_duration = self.config.get('test_duration', 3600)  # 1 hour default
        self.db = shelve.open(self.db_path)

    def _read_config(self, config_path):
        with open(config_path, 'r') as f:
            return json.load(f)

    def start_background_process(self):
        threading.Thread(target=self.tiering_check_loop, daemon=True).start()

    def tiering_check_loop(self):
        while True:
            self.perform_tiering_check()
            time.sleep(TIERING_CHECK_INTERVAL)

    def perform_tiering_check(self):
        logging.info("Starting tiering check")
        self.update_file_metadata()
        self.check_tier_capacities()
        self.move_files_based_on_rules()
        logging.info("Tiering check completed")

    def update_file_metadata(self):
        for tier in self.tiers:
            tier_path = os.path.join(self.mergerfs_mount, tier)
            for root, _, files in os.walk(tier_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, self.mergerfs_mount)
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
        for tier in self.tiers:
            tier_path = os.path.join(self.mergerfs_mount, tier)
            total, used, free = shutil.disk_usage(tier_path)
            usage_percent = (used / total) * 100
            if usage_percent > self.config.get('tier_capacity_threshold', 85):
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
            self.move_file(file_path, source_tier, target_tier)

    def move_files_based_on_rules(self):
        access_time_threshold = int(time.time()) - self.config.get('access_time_threshold', 28800)
        access_count_threshold = self.config.get('access_count_threshold', 3)
        
        files_to_move_up = [
            (k, v) for k, v in self.db.items()
            if v['access_count'] >= access_count_threshold and
            v['last_access_time'] > access_time_threshold and
            v['tier'] != "hot"
        ]
        
        for file_path, file_info in files_to_move_up:
            self.move_file(file_path, file_info['tier'], "hot")

    def move_file(self, file_path, source_tier, target_tier):
        source_path = os.path.join(self.mergerfs_mount, source_tier)
        target_path = os.path.join(self.mergerfs_mount, target_tier)
        relative_path = os.path.relpath(file_path, source_path)
        new_path = os.path.join(target_path, relative_path)
    
        os.makedirs(os.path.dirname(new_path), exist_ok=True)
        shutil.move(file_path, new_path)
    
        logging.info(f"Moved file from {file_path} to {new_path}")
    
        self.db[relative_path]['tier'] = target_tier
        self.db[relative_path]['last_tier_move'] = int(time.time())
        self.db.sync()

    def create_test_file(self, tier, filename):
        path = os.path.join(self.mergerfs_mount, tier, filename)
        with open(path, 'wb') as f:
            f.write(os.urandom(self.test_file_size))
        logging.info(f"Created test file: {path}")
        return path

    def access_file(self, path):
        with open(path, 'rb') as f:
            f.read(1024)  # Read just a bit to simulate access
        logging.info(f"Accessed file: {path}")

    def get_file_tier(self, filename):
        for tier in self.tiers:
            if os.path.exists(os.path.join(self.mergerfs_mount, tier, filename)):
                return tier
        return None

    def run_test(self):
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=self.test_duration)

        test_files = []
        for i in range(10):  # Create 10 test files
            tier = random.choice(self.tiers)
            filename = f"test_file_{i}.bin"
            path = self.create_test_file(tier, filename)
            test_files.append((filename, path, tier))

        while datetime.now() < end_time:
            for filename, path, original_tier in test_files:
                if random.random() < 0.3:  # 30% chance to access a file
                    self.access_file(path)
    
                current_tier = self.get_file_tier(filename)
                if current_tier != original_tier:
                    logging.info(f"File {filename} moved from {original_tier} to {current_tier}")

            time.sleep(60)  # Wait for a minute before next check

        self.verify_final_state(test_files)

    def verify_final_state(self, test_files):
        for filename, _, original_tier in test_files:
            final_tier = self.get_file_tier(filename)
            logging.info(f"File {filename}: Original tier: {original_tier}, Final tier: {final_tier}")
            
    def check_tier_usage(self):
        for tier in self.tiers:
            tier_path = os.path.join(self.mergerfs_mount, tier)
            total, used, free = shutil.disk_usage(tier_path)
            usage_percent = (used / total) * 100
            logging.info(f"{tier.capitalize()} tier usage: {usage_percent:.2f}%")

    def run_validation(self):
        logging.info("Starting tiering validation test")
        self.run_test()
        self.check_tier_usage()
        logging.info("Tiering validation test completed")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Validate tiering functionality")
    parser.add_argument("-c", "--config", default="/etc/drive-manager/validator_config.json", help="Path to the validator configuration file")
    args = parser.parse_args()

    validator = TieringValidator(args.config)
    validator.run_validation()
