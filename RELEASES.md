# Releases

This document describes the versioning and release process of OpenFGA. This document will be considered a living document. Scheduled releases, supported timelines, and API stability guarantees will be updated here as they change.

## Release Requirements
OpenFGA endeavours to rapidly release new features to the community.  To support that goal releases are performed based on the impact and quantity of updates to the codebase in a release-ready state.  Releases may be initiated based on a number of factors.

### Functional Changes
Changes ready for use in production environments which add new functionality or performance improvements may individually trigger an new release.

### Feature Previews
New features which are ready for testing by the community but are disabled unless OpenFGA is launched with a feature flag may also trigger a new release.

### Maintenance Updates
Minor updates, performance improvements, code refinements, or other items which do not introduce signifigant changes may not trigger a release individually, however when several minor changes have been merged a release may be initiated if no new version has been released in two weeks.

### Security Updates and Bugfixes
Changes made to address a known vulnerability or security issue or fixes to resolve a bug impacting users in production will trigger a release as soon as the changes have been tested and are considered production-ready.  A CVE will also be issued if relevant.

## Versioning
Releases of OpenFGA will be versioned using dotted triples. For the purposes of this document, we will refer to the respective components of this triple as \<major\>.\<minor\>.\<patch\>. When release candidates are published, they will include the suffix "-rc" followed by a release candidate number which identifies them as "pre-releases".

### Major and Minor Releases
Changes in the major and minor version components of the triple indicate changes in the functionality or performance of OpenFGA which may include breaking changes requiring those integrating OpenFGA in their project to make changes in their use of OpenFGA when adopting the new version.  It is recommended to fully review the published release notes accompanying a new release before upgrading.

### Patch Releases
Releases which increment only the "patch" component of the version triple may introduce new functionality, performance enhancements, bugfixes, or preview features while remaining backwards compatible with the previous release.  

## Release History
Current and previous releases can be found [here](https://github.com/openfga/openfga/releases).

## Roles

### Release Manager
One of the OpenFGA maintainers is designated as the release manager.  The release manager is tasked with:
- Communicating the release status with other maintainers 
- Reviewing the included PRs and compiling a descriptive changelog detailing the changes included in the new release using language that is meaningful and easy to understand for humans and ensures that contributors to the release are acknowledged 
- Discussing the level of testing that is needed and create a test plan if sensible including any areas where test coverage should be expanded
- Creating pull requests in the appropriate repositories and creating a new GitHub tag (ie. \<vX.Y.Z\>) indicating the version triple for the new release.
- Verifying the release's publication to GitHub, DockerHub, and the release of the associated Helm chart.
- Publishing release announcements or coordinating with other maintainers and community managers to publish release announcements to the openfga.dev website, the #openfga channel in the CNCF slack, and on other channels

In addition to these duties, if the release is a security fix the release manager will coordinate with relevant maintainers and contributors to create and publish a CVE detailing the identified vulnerabilites and their resolution.

## Artifacts

### Binaries

#### MacOS
- openfga_X.Y.Z_darwin_amd64.tar.gz
- openfga_X.Y.Z_darwin_amd64.tar.gz.sbom.json
- openfga_X.Y.Z_darwin_arm64.tar.gz
- openfga_X.Y.Z_darwin_arm64.tar.gz.sbom.json

#### Linux
- openfga_X.Y.Z_linux_386.tar.gz
- openfga_X.Y.Z_linux_386.tar.gz.sbom.json
- openfga_X.Y.Z_linux_amd64.tar.gz
- openfga_X.Y.Z_linux_amd64.tar.gz.sbom.json
- openfga_X.Y.Z_linux_arm64.tar.gz
- openfga_X.Y.Z_linux_arm64.tar.gz.sbom.json

#### Windows
- openfga_X.Y.Z_windows_386.tar.gz
- openfga_X.Y.Z_windows_386.tar.gz.sbom.json
- openfga_X.Y.Z_windows_amd64.tar.gz
- openfga_X.Y.Z_windows_amd64.tar.gz.sbom.json
- openfga_X.Y.Z_windows_arm64.tar.gz
- openfga_X.Y.Z_windows_arm64.tar.gz.sbom.json

### Container Images
An updated container image is published to https://hub.docker.com/r/openfga/openfga for each new OpenFGA release

### Helm Charts
Updated Helm Charts are published to https://openfga.github.io/helm-charts for each new OpenFGA release
