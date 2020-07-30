# IntelliJ setup

Before importing the project into IntelliJ, execute a clean build from the
command line. This helps setup the code properly.

## Import source

In IntelliJ:

* Either "Import project", or "File -> New -> Project from existing sources..."
* In the file browser, go the Graphflow source directory, and select the
'build.gradle' file. (This selection of the build.gradle file instead of just
the directory is important because it informs IntelliJ that this is a Gradle
project). In the 'Import Project from Gradle' dialog box, select 'use gradle
wrapper task configuration', and press 'OK'

This completes the basic setup of the project. You can browse the code. Follow
any IntelliJ prompts.

## Create run configurations

To run or debug the Server or the CLI in IntelliJ, we need to create Run
configurations.

* It is recommended that you link the provided configurations into the IntelliJ 
config directory, so that any changes made in the repo will be automatically 
applied by IntelliJ. From the root directory, run: `ln -s ../intellij_configs/runConfigurations .idea`
* Alternatively, copy the configurations: `cp -r intellij_configs/runConfigurations .idea`
* In IntelliJ, "Run -> Run... -> GraphflowServerRunner/GraphflowCliRunner"

To see/edit the actual configuration, go to "Run -> Edit Configurations..."

## Import style settings

Import the IntelliJ code style configuration for auto-formatting code.

* "File -> Settings -> Editor -> Code Style -> Java -> Manage... -> Import... -> IntelliJ IDEA code style XML"
* Browse to the source directory and select 'intellij_configs/graphflow_code_style_guide.xml'
* Press "OK" everywhere and exit the settings.

You can now use 'Alt + Shift + L' to format code automatically in IntelliJ.

## Code inspections

Import `intellij_configs/graphflow_inspections.xml` using "File -> Inspections -> Profile (gear icon) -> 
Import profile..." into IntelliJ to highlight code that deviates from the project style guidelines.

## Known Issues and Solutions

- You may run into the following error when trying to build if your IntelliJ version is outdated:
```
Exception in thread “main” java.lang.NoClassDefFoundError: io/grpc/BindableService
```
This is detailed [here](https://intellij-support.jetbrains.com/hc/en-us/community/posts/115000364850-Gradle-integration-external-dependencies-not-in-classpath-on-run-debug).
Simply update your IntelliJ to the 2017+ version.

- The gradle version is updated regularly. This can cause various build issues at times on IntelliJ if it is
not refreshed from the Gradle projects in the right side pane.
