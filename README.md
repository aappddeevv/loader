##Purpose
An application that loads data into an RDBMS
using parallel loads and a simple DSL inspired by ETL tools
to specify the attribute mappings.

##History

The project started out many years ago as a java program
hosted on sourceforge but I moved it over to github and
updated it to use scala about a year ago. Its been tested
in production environments to be "good enough" to work on
large loads. If you can, use your ETL tool or bulk loaders
specific to your RDBMS, but otherwise you may find this
simple application useful.

##Mappings Development
Create a new sbt project then include this project
as a dependency. 

You must first publish this project locally using
```sh
sbt publishLocal
```
then add the published file as a dependency to your
project.
```scala
libraryDependencies ++= Seq(
"org.im" %% "loader" % "latest.version"
)
```
Once you have specified this project as a dependency
you need to:
1 Create your main program
2 Create your command line options. There are some
available to you using the program.parser value.
3 Develop your mappings (see below)
4 Call the program.runloader function providing
your command line parser (derived from (2)), the
default configuration with your list of mappings
and the command line arguments.

That's it!

Tip: To create a command line parser from the one
provided in the program object just do:
```scala
val yourparser = new scopt.OptionParser[Config]("loader") { 
   options ++= program.parser.stdargs // don't retype them
   ...
   <more of your options here>
}
```

##Mapping Development
To create your mappings, derive from the mappings
object in org.im.loader and specify mappings using
the DSL.
```scala
object table1mappings extends mappings("table1", "mappings", Some("theschema")) {
    import sourcefirst._
    import org.im.loader.Implicits._
    import com.lucidchart.open.relate.interp.Parameter._ 

    string("cola").directMove
    long("colb)".to("colbtarget")
    ...
    to[Long]("colc").rule(0){ ctx =>
        ctx.success(ctx.input.get("funkycolcsource"))
    }
}
```
You can also define the schema in the mappings to help
with type conversions before your rule receive your data.

"Source first" mappings are mappings that start with the
source such as `string("cola")`. That says that the mapping
should have the source attribute in the input record to
be `cola`. It's better to specify a target first mapping
using `to[..](..)` and then specify rules. Rules
have a priority and are run in priority order. See the
dsltests.scala file in the test directory for examples
of mappings and how to specify the rules.


##Mapping Testing
The typical development  model is to leave your project open
in your editor, edit your mappings, then run the load from
the sbt command line for unit tests. Once, the mappings
are complete, bundle up "your" project and deploy it. Since
the library is not deployed to maven, just download it,
add your IDE's configuration using
```sh
sbt eclipse with-source=true
```
then develop and test your mappings, then deploy them via
a zip file.

Check out the `dsltest.scala` test file for examples of how
to specify your mappings.

You will want to drop your favorite jdbc lib into the lib directory
or include it in the dependencies inside build.sbt.



##Deploying

The application can be packaged by typing
```sh
sbt universal:packageBin 
```
to obtain a zip file that can be installed, however, it is
really designed to a library that you embed in your
application so that you can provide the statically compiled
mappings. 

