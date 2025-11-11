{
  description = "Minimal Rust development environment with Nix flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      utils,
    }:
    utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ] (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        inherit (nixpkgs) lib;
        runtimeDependencies = with pkgs; [

        ];

        nativeBuildInputs = with pkgs; [
          # pkg-config
        ];

        buildInputs = with pkgs; [
          codecrafters-cli
          cargo
          rustc
          rustfmt
        ];

      in
      {
        devShells.default = pkgs.mkShell {
          inherit buildInputs nativeBuildInputs runtimeDependencies;

          LD_LIBRARY_PATH = lib.makeLibraryPath (buildInputs ++ nativeBuildInputs ++ runtimeDependencies);
        };
      }
    );
}
