import NIO
import MongoClient
import MongoCore

extension MongoCollection {
    public func updateOne(
        where query: Document,
        to document: Document,
        file: StaticString = #file, line: UInt = #line
    ) -> EventLoopFuture<UpdateReply> {
        return pool.next(for: .basic).flatMap { connection in
            let request = UpdateCommand.UpdateRequest(where: query, to: document)
            let command = UpdateCommand(updates: [request], inCollection: self.name)
            
            return connection.executeCodable(
                command,
                namespace: self.database.commandNamespace,
                in: self.transaction,
                sessionId: self.sessionId ?? connection.implicitSessionId,
                metadata: CommandMetadata(file: file, line: line)
            )
        }.decode(UpdateReply.self)._mongoHop(to: hoppedEventLoop)
    }
    
    public func updateOne<Query: MongoKittenQuery>(
        where query: Query,
        to document: Document,
        file: StaticString = #file, line: UInt = #line
    ) -> EventLoopFuture<UpdateReply> {
        return updateOne(
            where: query.makeDocument(),
            to: document,
            file: file,
            line: line
        )
    }
    
    public func updateMany(
        where query: Document,
        to document: Document,
        file: StaticString = #file, line: UInt = #line
    ) -> EventLoopFuture<UpdateReply> {
        return pool.next(for: .basic).flatMap { connection in
            var request = UpdateCommand.UpdateRequest(where: query, to: document)
            request.multi = true
            let command = UpdateCommand(updates: [request], inCollection: self.name)
            
            return connection.executeCodable(
                command,
                namespace: self.database.commandNamespace,
                in: self.transaction,
                sessionId: self.sessionId ?? connection.implicitSessionId,
                metadata: CommandMetadata(file: file, line: line)
            )
        }.decode(UpdateReply.self)._mongoHop(to: hoppedEventLoop)
    }
    
    public func updateMany<Query: MongoKittenQuery>(
        where query: Query,
        to document: Document,
        file: StaticString = #file, line: UInt = #line
    ) -> EventLoopFuture<UpdateReply> {
        return updateMany(
            where: query.makeDocument(),
            to: document,
            file: file,
            line: line
        )
    }
    
    public func updateMany(
        where query: Document,
        setting: Document?,
        unsetting: Document?,
        file: StaticString = #file, line: UInt = #line
    ) -> EventLoopFuture<UpdateReply> {
        return pool.next(for: .basic).flatMap { connection in
            var request = UpdateCommand.UpdateRequest(where: query, setting: setting, unsetting: unsetting)
            request.multi = true
            
            let command = UpdateCommand(updates: [request], inCollection: self.name)
            
            return connection.executeCodable(
                command,
                namespace: self.database.commandNamespace,
                in: self.transaction,
                sessionId: self.sessionId ?? connection.implicitSessionId,
                metadata: CommandMetadata(file: file, line: line)
            )
        }.decode(UpdateReply.self)._mongoHop(to: hoppedEventLoop)
    }
    
    public func upsert(
        _ document: Document,
        where query: Document,
        file: StaticString = #file, line: UInt = #line
    ) -> EventLoopFuture<UpdateReply> {
        return pool.next(for: .basic).flatMap { connection in
            var request = UpdateCommand.UpdateRequest(where: query, to: document)
            request.multi = false
            request.upsert = true
            
            let command = UpdateCommand(updates: [request], inCollection: self.name)
            
            return connection.executeCodable(
                command,
                namespace: self.database.commandNamespace,
                in: self.transaction,
                sessionId: self.sessionId ?? connection.implicitSessionId,
                metadata: CommandMetadata(file: file, line: line)
            )
        }.decode(UpdateReply.self)._mongoHop(to: hoppedEventLoop)
    }
}
