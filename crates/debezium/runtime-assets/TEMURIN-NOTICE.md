# Eclipse Temurin runtime notice

This bundle repackages the Eclipse Temurin JRE 21.0.12.1+1 runtime without
changing file payloads. Internal license symlinks are materialized as regular
files so the final bundle has a closed, checksum-verifiable file set. Its
notices and license texts are retained under `runtime/NOTICE` and
`runtime/legal/`. The upstream CycloneDX document is retained as
`runtime-sbom.json`.

The corresponding OpenJDK source archive is:

```text
https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.12.1%2B1/OpenJDK21U-jdk-sources_21.0.12.1_1.tar.gz
SHA-256 573057d03584ae793fb7ec9a14c76d826d9187a53efeefd99da47403a5308234
```

DogPaddle does not modify the JRE file payloads. Release owners must preserve
this notice, the runtime's upstream notices, the source reference, and the
bundle checksum manifest when redistributing it.
