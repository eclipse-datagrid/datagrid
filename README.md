
# Eclipse Data Grid

## Description

Eclipse Data Grid is an in-memory data processing layer to speed up database applications and relieve the database.

Eclipse Data Grid can be used as a traditional distributed cache, but it is much more than a common cache. It enables you ultra-fast in-memory searching, as well as complex data processing, through the implementation of individual Java business logic. Unlike traditional caching solutions, which are built as key-value structures, Eclipse Data Grid is a native Java layer that utilizes the native Java object model. This allows you to work with native Java objects, Java types, and any complex Java object graphs, as well as integrate any Java libraries and implement complex logic in your in-memory data layer using Java.

Eclipse Data Grid is for everyone who needs an easy-to-use distributed cache, as well as for users who need much more than just a cache, combining caching, high-speed in-memory searching, and complex data manipulation by using Core Java concepts. Move your complex and performance-critical data and data operations to Eclipse Data Grid to significantly reduce database workloads and save costs, boost your application, and your business

Eclipse Data Grid is based on two other Eclipse projects:

- [Eclipse Serializer](https://github.com/eclipse-serializer/serializer)

    Powerful and highly secure Java serialization that enables dealing with any complex Java object graphs and avoids deserialization attacks by injecting and executing malicious code.

- [EclipseStore](https://github.com/eclipse-store/store)

  Java-native object graph persistence layer to store any complex Java object graphs or individual subgraphs transaction-safe into any binary data storage, and restore them in RAM on demand. Using a traditional database and thus OR-Mapping, JSON conversion, or any other mappings are completely superfluous. EclipseStore is ACID-compliant, provides lazy-loading, indexing, GigaMap for fully automated lazy-loading, and provides a smart concept for schema migration. EclipseStore is built as a persistence layer to be used for a single JVM run on a single node.

Eclipse Data Grid itself provides you with the code to generate a cluster environment to run, scale, and maintain an Eclipse Data Grid application based on Kubernetes, as well as important cluster features such as replication, elastic scale-out / scale-in, and backups. Eclipse Data Grid bases on a single-writer approach. While the consistency model on each cluster node is full consistency, the standard cluster consistency model is eventual consistency.

## License

Eclipse Data Grid is available under [Eclipse Public License - v 2.0](LICENSE).
