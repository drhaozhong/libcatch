# Compiler-directed Migration of Client Code

## Project summary

From the side of an API library, its programmers take much effort in repairing its bugs and implementing new APIs. Although newer versions of libraries typically have fewer bugs and more functionalities, many changes in libraries can break their API interfaces. As a result, from the side of client-code programmers, it often is tedious and error-prone to migrate their code, because new libraries contain many API breaking changes (e.g., deleting an API). After replacing a library, such breaking changes introduce many compilation errors, and programmers have to resolve these errors in client code. In the literature, researchers have proposed various approaches to automate the migration process. However, the prior approaches can either mine only API mappings or migrate code with change examples. 


The migration of real code often involves complicated edits. The prior approaches learn transformation rules from given change examples of either API code or client code. Without change examples, they cannot synthesize complicated edits that are required by migrating real code. APIs are many. In addition, the source and target versions must be exactly match the code under migration. For example, if a library is updated from 1.0 to 3.0, the examples from 2.0 to 3.0 are less useful, since their APIs can be quite different. As a result, it is infeasible to construct sufficient change examples for migration. 


In this paper, we propose a novel approach, called LibCatch, to further improve the state of the art. It does not need any change examples. Instead, its basic idea is to synthesize complicated edits from simpler ones, under the guidance of its migration algorithm. 


## Setting
### Archive
In our evaluation, we use the examples of accumulo, cassandra, karaf, lucene, and poi to construct migration tasks. The source files and binary files of the above projects are collected from their archives:

accumulo: https://archive.apache.org/dist/accumulo/

cassandra: https://archive.apache.org/dist/cassandra/

karaf: https://archive.apache.org/dist/karaf/

lucene: https://archive.apache.org/dist/lucene/java/

poi: https://archive.apache.org/dist/poi/release/

### API
All the above projects provide APIs, and their API documents are shipped with their binary files. For example, the API documents of cassandra 1.0.0 can be obtained with the following steps:
1. Download apache-cassandra-1.0.0-beta1-bin.tar.gz from https://archive.apache.org/dist/cassandra/1.0.0/
2. Unzip apache-cassandra-1.0.0-beta1-bin.tar.gz
3. The API documents are under the javadoc folder. 


## Migration on our Benchmark
In each task, we compilie an example of v1 with the binary files of v2 reveals the compilation errors. When programmers manually migrate the examples from v1 to v2, they must resolve all the compilation errors. 
The migration results on our benchmark are listed under the benchmark folder.

## Migration on Real Projects
We used our tool to migrate 15 real projects. The original projects and our migrated resutls are listed in the realproject folder.

