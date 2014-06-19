## mini-dev-cluster
================

Mini ### YARN/DFS cluster for developing and testing YARN-based applications (e.g., Tez)


==
#### Configure the development environment.
Clone the project first. 

`git clone https://github.com/hortonworks/mini-dev-cluster.git`

Once cloned, navigate to the project's root `cd mini-dev-cluster` and follow these instructions. You should be up and running in less then a min. 

This project uses _Gradle_ as its build and dependency management (see http://www.gradle.org/). _Gradle_ is self-provisioning build framework which means you don't have to have _Gradle_ installed to follow the rest of the procedure. 

BUILD for development:

Depending on the IDE you are using execute the following _gradle_ script.

##### Eclipse:

	./gradlew clean eclipse
	
##### IntelliJ IDEA

	./gradlew clean idea
	
The above will generate all necessary IDE-specific artifacts to successfully import the project.
Import the project into your IDE.
For example in Eclipse follow this procedure:

	File -> Import -> General -> Existing Project Into Workspace -> browse to the root of the project and click Finish
> NOTE: You don't have to import projects as Gradle and/or Maven project. The `./gradlew clean eclipse/idea` command will take care of generating all the necessary IDE-native artifacts so you can import it as Eclipse/Gradle project and not deal with wrong plugin versions of one or another.
 

**_This is an evolving work in progress so more updates (code and documentation) will be coming soon_**
