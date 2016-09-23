import sbt._

object MyBuild extends Build {

  lazy val root = Project("root", file(".")) dependsOn(aerospikeHelperJava)
  lazy val aerospikeHelperJava =
       RootProject(uri("git://github.com/aerospike/aerospike-helper.git"))

}