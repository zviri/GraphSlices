# GraphSlices
GraphSlices is a versatile framework designed for the analysis of dynamic networks. The framework focuses on three key areas to achieve its goals:

1. Rich and High-Performance Graph Abstraction:
   * The core of GraphSlices is the Graph interface, encapsulating a dynamic graph and providing methods for graph manipulation.
   * Two implementations of the Graph interface exist: a serial one using plain Scala core collections, and a parallel one leveraging Scala’s parallel collections.
   * Graph computation involves chaining operations on the Graph interface, with each operation returning a transformed version of the original graph.
2. Graph Mining Algorithms:
   * GraphSlices includes a comprehensive collection of graph mining algorithms, allowing users to perform various analyses on dynamic graphs.
3. Synthetic Graph Generation and Data Loading:
   * The framework supports the generation of synthetic graphs through the Generators module, which includes methods for creating standard classes of artificial graphs.
   * Graph loading is facilitated by the Builders module, offering methods to load existing graphs from common formats such as edge lists, CSV files, or GraphML.

## Architecture Overview
### Graph Interface:
* Central to GraphSlices is the Graph interface, responsible for all graph manipulation operations.
* Two basic implementations are supported: a serial single-threaded implementation and a parallel multi-threaded implementation.
### Modules:
* Generators: Provides methods for generating various classes of artificial graphs.
* Algorithms: Contains implementations of common graph algorithms for link analysis, graph structure analysis, and clustering.
* Builders: Handles the loading of graphs from different formats.

## Compilation
`sbt compile`
