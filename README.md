This is a small application that loads data into an RDBMS
using parallel loads and a simple DSL inspired by ETL tools
to specify the attribute mappings.

The project started out many years ago as a java program
hosted on sourceforge but I moved it over to github and
updated it to use scala about a year ago. Its been tested
in production environments to be "good enough" to work on
large loads. If you can, use your ETL tool or bulk loaders
specific to your RDBMS, but otherwise you may find this
simple application useful.

The application can be packaged by typing
```sh
sbt universal:packageBin 
```
to obtain a zip file that can be installed, however, it is
really designed to a library that you embed in your
application so that you can provide the statically compiled
mappings. The typical model is to leave your project open
in your editor, edit your mappings, then run the load from
the sbt command line for unit tests. Then once, the mappings
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
