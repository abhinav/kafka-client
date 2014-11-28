{ pkgs ? import <nixpkgs> {}
, haskellPackages ? pkgs.haskellPackages
}:

haskellPackages.callPackage ./. {
    cabal = haskellPackages.cabal.override {
        extension = self: super: {
            doCheck = true;
        };
    };
}
