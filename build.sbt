enablePlugins(JavaAppPackaging)
enablePlugins(JDKPackagerPlugin)
enablePlugins(UniversalPlugin)


name := "loader"
organization := "org.im"
version := "0.1.0"
scalaVersion := "2.11.8"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers in ThisBuild += Resolver.url("file://" + Path.userHome.absolutePath + "/.ivy/local")
resolvers in ThisBuild += Resolver.sonatypeRepo("releases")
resolvers in ThisBuild += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
resolvers in ThisBuild += Resolver.bintrayRepo("scalaz", "releases")
resolvers in ThisBuild += Resolver.jcenterRepo


val deps = Seq(
  "org.scalatest" %% "scalatest" %  "latest.release" % "test"
    ,"org.scala-lang.modules" %% "scala-xml" % "latest.release"
    ,"com.typesafe" % "config" %  "latest.release"
    ,"com.github.scopt" %% "scopt" % "latest.release"
    ,"ch.qos.logback" % "logback-classic" % "latest.release"
    ,"ch.qos.logback" % "logback-core" % "latest.release"
    ,"net.databinder.dispatch" %% "dispatch-core" % "latest.release"
    ,"commons-codec" % "commons-codec" % "latest.release"
    ,"org.scala-lang.modules" %% "scala-async" % "latest.release"
    ,"org.log4s" %% "log4s" % "latest.release"
    ,"com.github.pathikrit" %% "better-files" % "latest.release"
    ,"com.iheart" %% "ficus" % "latest.version"
    ,"org.typelevel" %% "cats" % "0.7.2"
    ,"co.fs2" %% "fs2-core" % "latest.version"
    ,"co.fs2" %% "fs2-io" % "latest.version"
    ,"com.zaxxer" % "HikariCP" % "latest.version"
    ,"com.lucidchart" %% "relate" % "latest.version"
    ,"com.bizo" %% "mighty-csv" % "latest.version"
    ,"net.sf.opencsv" % "opencsv" % "latest.version"
	,"org.apache.commons" % "commons-lang3" % "latest.release"
)


lazy val commonSettings = Seq(
  organization := "org.im",
  version := "0.1.0",
  scalaVersion := "2.11.8",
  EclipseKeys.useProjectId := true,
  EclipseKeys.withSource := true,
  EclipseKeys.skipParents in ThisBuild := false,
  EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource)

lazy val root = (project in file(".")).
  aggregate(core, csv, spark)
  
lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(name := "loader-core").
  settings(libraryDependencies ++= deps)

lazy val csv = (project in file("csv")).
  settings(commonSettings: _*).
  settings(name := "loader-csv").
  dependsOn(core).
  settings(libraryDependencies ++= deps)
  
lazy val spark = (project in file("spark")).
  settings(commonSettings: _*).
  settings(name := "loader-spark").
  dependsOn(core)


EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true


fork in run := true

javaOptions in run += "-Xmx4G"

