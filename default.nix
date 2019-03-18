{ pkgs ? ((import <nixpkgs> {}).pkgs), compiler ? "default" }:

pkgs.stdenv.mkDerivation rec {
  name = "tds-env";
  version = "0.0.0.1";
  buildInputs = [
    pkgs.stack
    pkgs.sqsh
    pkgs.zlib.dev
    pkgs.zlib.out
    pkgs.pkgconfig
  ];
}
