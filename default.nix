# This file was auto-generated by cabal2nix. Please do NOT edit manually!

{ cabal, cereal, digest, hspec, network, time }:

cabal.mkDerivation (self: {
  pname = "kafka-client";
  version = "0.1.0.0";
  src = ./.;
  buildDepends = [ cereal digest network time ];
  testDepends = [ hspec ];
  meta = {
    description = "Haskell client library for Apache Kafka";
    license = self.stdenv.lib.licenses.mit;
    platforms = self.ghc.meta.platforms;
  };
})
