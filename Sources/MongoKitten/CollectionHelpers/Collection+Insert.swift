import MongoClient
import NIO
import MongoCore

extension MongoCollection {
    public func insert(_ document: Document, file: StaticString = #file, line: UInt = #line) -> EventLoopFuture<InsertReply> {
        return insertMany([document], file: file, line: line)
    }
    
    public func insertManyEncoded<E: Encodable>(_ models: [E], file: StaticString = #file, line: UInt = #line) -> EventLoopFuture<InsertReply> {
        do {
            let documents = try models.map { model in
                return try BSONEncoder().encode(model)
            }
            
            return insertMany(documents, file: file, line: line)
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    public func insertEncoded<E: Encodable>(_ model: E, file: StaticString = #file, line: UInt = #line) -> EventLoopFuture<InsertReply> {
        do {
            let document = try BSONEncoder().encode(model)
            return insert(document, file: file, line: line)
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    public func insertMany(_ documents: [Document], file: StaticString = #file, line: UInt = #line) -> EventLoopFuture<InsertReply> {
        return pool.next(for: .writable).flatMap { connection in
            let command = InsertCommand(documents: documents, inCollection: self.name)
            
            return connection.executeCodable(
                command,
                namespace: self.database.commandNamespace,
                in: self.transaction,
                sessionId: self.sessionId ?? connection.implicitSessionId,
                metadata: CommandMetadata(file: file, line: line)
            )
        }.decode(InsertReply.self).flatMapThrowing { reply in
            if reply.ok == 1 {
                return reply
            }

            self.pool.logger.error("MongoDB Insert operation failed")
            throw reply
        }._mongoHop(to: hoppedEventLoop)
    }
}
