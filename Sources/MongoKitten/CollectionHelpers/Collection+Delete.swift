import MongoClient
import MongoCore
import NIO

extension MongoCollection {
    public func deleteOne(where query: Document, file: StaticString = #file, line: UInt = #line) -> EventLoopFuture<DeleteReply> {
        return pool.next(for: .writable).flatMap { connection in
            return connection.executeCodable(
                DeleteCommand(where: query, limit: .one, fromCollection: self.name),
                namespace: self.database.commandNamespace,
                in: self.transaction,
                sessionId: self.sessionId ?? connection.implicitSessionId,
                metadata: CommandMetadata(file: file, line: line)
            )
        }.decode(DeleteReply.self)._mongoHop(to: hoppedEventLoop)
    }
    
    public func deleteAll(where query: Document, file: StaticString = #file, line: UInt = #line) -> EventLoopFuture<DeleteReply> {
        return pool.next(for: .writable).flatMap { connection in
            return connection.executeCodable(
                DeleteCommand(where: query, limit: .all, fromCollection: self.name),
                namespace: self.database.commandNamespace,
                in: self.transaction,
                sessionId: connection.implicitSessionId,
                metadata: CommandMetadata(file: file, line: line)
            )
        }.decode(DeleteReply.self)._mongoHop(to: hoppedEventLoop)
    }
    
    public func deleteOne<Q: MongoKittenQuery>(where filter: Q, file: StaticString = #file, line: UInt = #line) -> EventLoopFuture<DeleteReply> {
        return self.deleteOne(where: filter.makeDocument(), file: file, line: line)
    }
    
    public func deleteAll<Q: MongoKittenQuery>(where filter: Q, file: StaticString = #file, line: UInt = #line) -> EventLoopFuture<DeleteReply> {
        return self.deleteAll(where: filter.makeDocument(), file: file, line: line)
    }
}
