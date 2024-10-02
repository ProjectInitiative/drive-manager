{
  description = "Drive Manager development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        driveManager = pkgs.callPackage ./default.nix {};
      in
      {
        packages.default = driveManager;

        devShells.default = pkgs.mkShell {
          inputsFrom = [ driveManager ];
          buildInputs = with pkgs; [
            (rust-bin.stable.latest.default.override {
              extensions = [ "rust-src" ];
            })
          ] ++ driveManager.propagatedBuildInputs;

          shellHook = ''
            echo "Drive Manager development environment"
            echo "Run 'cargo build' to compile the project"
          '';
        };
      }
    );
}
