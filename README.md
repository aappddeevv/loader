##Purpose
An application that loads data into an RDBMS
using parallel loads and a simple DSL inspired by ETL tools
to specify the attribute mappings.

##History

The project started out many years ago as a java program
hosted on sourceforge but I moved it over to github and
updated it to use scala about a year ago. I received
a large number of private updates from various versions
in between and recently was prompted privately to publish
them into github.

Its been tested
in production environments to be "good enough" to work on
large loads. Use your ETL tool or bulk loaders
specific to your RDBMS first, but otherwise you may find this
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
* Create your main program
* Create your command line options. There are some
options available to you using the program.parser value.
* Develop your mappings (see below).
* Call the `program.runloader(..)` function providing
your command line parser (derived from (2)), the
default configuration derived from `org.im.loader.Config`
with your list of mappings and the command arguments
from your main class.

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
object table1mappings extends mappings("table1", "table1", Some("theschema")) {
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
Subclassing the mappings object allows you to add your
convenience combinator methods to the mappings object.
For example, you could add a 'lookup' combinator or
a `.directMoveButOnlyUnderCertainConditions` combinator.

"Source first" mappings are mappings that start with the
source such as `string("cola")`. That says that the mapping
should have the source attribute come from the attribute `cola1`
in the input record. 

It's better to specify a "target first" mapping
such as `to[..](..)` and then specify processing rules. Rules
have a priority and are run in priority order. See the
dsltests.scala file in the test directory for examples
of mappings and how to specify the rules.


##Mapping Testing
The typical development  model is to leave your project open
in your editor, edit your mappings, then run the load from
the sbt command line for unit tests. Once the mappings
are complete, bundle up "your" project and deploy it. Since
this library is not deployed to maven, download it,
then create your IDE's configuration using
```sh
sbt eclipse with-source=true
```
Develop and test your mappings. Then deploy the entire
application via a zip file.

Check out the `dsltest.scala` test file for examples of how
to specify your mappings.

You will want to drop your favorite jdbc lib into the lib directory
or include it in the dependencies inside build.sbt.


##Deploying

The application can be packaged by typing
```sh
sbt universal:packageBin 
```
to obtain a zip file that can be installed. You will want
to have the same plugins specified in this library
in your own project's project/plugins.sbt to make this work.

