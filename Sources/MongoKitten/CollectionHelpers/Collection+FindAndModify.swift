import NIO
import MongoClient
import MongoKittenCore

extension MongoCollection {
    // MARK: - Builder Functions (Composable/Chained API)
    
    /// Modifies and returns a single document.
    /// - Parameters:
    ///   - query: The selection criteria for the modification.
    ///   - update: If passed a document with update operator expressions, performs the specified modification. If passed a replacement document performs a replacement.
    ///   - remove: Removes the document specified in the query field. Defaults to `false`
    ///   - returnValue: Wether to return the `original` or `modified` document.
    public func findAndModify(
        where query: Document,
        update document: Document = [:],
        remove: Bool = false,
        returnValue: FindAndModifyReturnValue = .original
    ) -> FindAndModifyBuilder {
        var command = FindAndModifyCommand(collection: self.name, query: query)
        command.update = document
        command.remove = remove
        command.new = returnValue == .modified
        return FindAndModifyBuilder(command: command, collection: self)
    }
    
    /// Deletes a single document based on the query, returning the deleted document.
    /// - Parameters:
    ///   - query: The selection criteria for the deletion.
    public func findOneAndDelete(where query: Document) -> FindAndModifyBuilder {
        var command = FindAndModifyCommand(collection: self.name, query: query)
        command.remove = true
        return FindAndModifyBuilder(command: command, collection: self)
    }
    
    /// Replaces a single document based on the specified query.
    /// - Parameters:
    ///   - query: The selection criteria for the upate.
    ///   - replacement: The replacement document.
    ///   - returnValue: Wether to return the `original` or `modified` document.
    public func findOneAndReplace(
        where query: Document,
        replacement document: Document,
        returnValue: FindAndModifyReturnValue = .original
    ) -> FindAndModifyBuilder {
        var command = FindAndModifyCommand(collection: self.name, query: query)
        command.new = returnValue == .modified
        command.update = document
        return FindAndModifyBuilder(command: command, collection: self)
    }
    
    /// Replaces a single document based on the specified query.
    /// - Parameters:
    ///   - query: The selection criteria for the upate.
    ///   - replacement: The replacement document.
    ///   - returnValue: Wether to return the `original` or `modified` document.
    public func findOneAndUpsert(
        where query: Document,
        replacement document: Document,
        returnValue: FindAndModifyReturnValue = .original
    ) -> FindAndModifyBuilder {
        var command = FindAndModifyCommand(collection: self.name, query: query)
        command.new = returnValue == .modified
        command.update = document
        command.upsert = true
        return FindAndModifyBuilder(command: command, collection: self)
    }
    
    /// Updates a single document based on the specified query.
    /// - Parameters:
    ///   - query: The selection criteria for the upate.
    ///   - document: The update document.
    ///   - returnValue: Wether to return the `original` or `modified` document.
    public func findOneAndUpdate(
        where query: Document,
        to document: Document,
        returnValue: FindAndModifyReturnValue = .original
    ) -> FindAndModifyBuilder {
        var command = FindAndModifyCommand(collection: self.name, query: query)
        command.new = returnValue == .modified
        command.update = document
        return FindAndModifyBuilder(command: command, collection: self)
    }
    
    /// Modifies and returns a single document.
    /// - Parameters:
    ///   - query: The selection criteria for the modification.
    ///   - update: If passed a document with update operator expressions, performs the specified modification. If passed a replacement document performs a replacement.
    ///   - remove: Removes the document specified in the query field. Defaults to `false`
    ///   - returnValue: Wether to return the `original` or `modified` document.
    public func findAndModify<Query: MongoKittenQuery>(
        where query: Query,
        update document: Document = [:],
        remove: Bool = false,
        returnValue: FindAndModifyReturnValue = .original
    ) -> FindAndModifyBuilder {
        var command = FindAndModifyCommand(collection: self.name, query: query.makeDocument())
        command.update = document
        command.remove = remove
        command.new = returnValue == .modified
        return FindAndModifyBuilder(command: command, collection: self)
    }
    
    /// Deletes a single document based on the query, returning the deleted document.
    /// - Parameters:
    ///   - query: The selection criteria for the deletion.
    public func findOneAndDelete<Query: MongoKittenQuery>(
        where query: Query
    ) -> FindAndModifyBuilder {
        var command = FindAndModifyCommand(collection: self.name, query: query.makeDocument())
        command.remove = true
        return FindAndModifyBuilder(command: command, collection: self)
    }
    
    /// Replaces a single document based on the specified query.
    /// - Parameters:
    ///   - query: The selection criteria for the upate.
    ///   - replacement: The replacement document.
    ///   - returnValue: Wether to return the `original` or `modified` document.
    public func findOneAndReplace<Query: MongoKittenQuery>(
        where query: Query,
        replacement document: Document,
        returnValue: FindAndModifyReturnValue = .original
    ) -> FindAndModifyBuilder {
        var command = FindAndModifyCommand(collection: self.name, query: query.makeDocument())
        command.new = returnValue == .modified
        command.update = document
        return FindAndModifyBuilder(command: command, collection: self)
    }
    
    /// Updates a single document based on the specified query.
    /// - Parameters:
    ///   - query: The selection criteria for the upate.
    ///   - document: The update document.
    ///   - returnValue: Wether to return the `original` or `modified` document.
    public func findOneAndUpdate<Query: MongoKittenQuery>(
        where query: Query,
        to document: Document,
        returnValue: FindAndModifyReturnValue = .original
    ) -> FindAndModifyBuilder {
        var command = FindAndModifyCommand(collection: self.name, query: query.makeDocument())
        command.new = returnValue == .modified
        command.update = document
        return FindAndModifyBuilder(command: command, collection: self)
    }
}

public final class FindAndModifyBuilder {
    /// The underlying command to be executed.
    public var command: FindAndModifyCommand
    private let collection: MongoCollection
    
    init(command: FindAndModifyCommand, collection: MongoCollection) {
        self.command = command
        self.collection = collection
    }
    
    /// Executes the command
    public func execute() async throws -> FindAndModifyReply {
        let connection = try await collection.pool.next(for: .writable)
        return try await connection.executeCodable(
            self.command,
            decodeAs: FindAndModifyReply.self,
            namespace: self.collection.database.commandNamespace,
            in: self.collection.transaction,
            sessionId: self.collection.sessionId ?? connection.implicitSessionId
        )
    }
    
    public func decode<D: Decodable>(_ type: D.Type) async throws -> D? {
        try await self.execute().value.map { document in
            try BSONDecoder().decode(D.self, from: document)
        }
    }
    
    public func sort(_ sort: Sorting) -> FindAndModifyBuilder {
        self.command.sort = sort.document
        return self
    }
    
    public func sort(_ sort: Document) -> FindAndModifyBuilder {
        self.command.sort = sort
        return self
    }
    
    public func project(_ projection: Projection) -> FindAndModifyBuilder {
        self.command.fields = projection.document
        return self
    }
    
    public func project(_ projection: Document) -> FindAndModifyBuilder {
        self.command.fields = projection
        return self
    }
    
    public func writeConcern(_ concern: WriteConcern) -> FindAndModifyBuilder {
        self.command.writeConcern = concern
        return self
    }
    
    public func collation(_ collation: Collation) -> FindAndModifyBuilder {
        self.command.collation = collation
        return self
    }
}
