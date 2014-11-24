{ pkgs ? import <nixpkgs> {}
, haskellPackages ? pkgs.haskellPackages
}:

with haskellPackages; callPackage ./. {}
