enablePlugins(JavaAppPackaging)
enablePlugins(JDKPackagerPlugin)
enablePlugins(UniversalPlugin)


name := "loader"
organization := "org.im"
version := "0.1.0"
scalaVersion := "2.11.8"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers += Resolver.url("file://" + Path.userHome.absolutePath + "/.ivy/local")
resolvers += Resolver.sonatypeRepo("releases")
resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
resolvers += Resolver.bintrayRepo("scalaz", "releases")
resolvers += Resolver.jcenterRepo


libraryDependencies ++= Seq(
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

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true


fork in run := true

javaOptions in run += "-Xmx4G"

