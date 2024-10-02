{ lib
, pkgs
, rustPlatform
}:

rustPlatform.buildRustPackage rec {
  pname = "drive-manager";
  version = "0.1.0";

  src = ./.;

  cargoLock = {
    lockFile = ./Cargo.lock;
  };

  nativeBuildInputs = with pkgs; [
    pkg-config
  ];

  buildInputs = with pkgs; [
    sqlite
  ];

  # System utilities required by the program
  propagatedBuildInputs = with pkgs; [
    mergerfs
    fuse
    util-linux  # Provides lsblk
    parted      # Provides fdisk functionality
    e2fsprogs   # Provides mkfs
    rsync
  ];

  meta = with lib; {
    description = "A drive management and tiering system";
    homepage = "https://github.com/projectinitiative/drive-manager";
    license = licenses.mit;
    maintainers = [ maintainers.projectinitiative ];
  };
}
