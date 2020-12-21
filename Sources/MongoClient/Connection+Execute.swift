import Foundation
import Metrics
import BSON
import MongoCore
import NIO

extension MongoConnection {
    public func executeCodable<E: Encodable>(
        _ command: E,
        namespace: MongoNamespace,
        in transaction: MongoTransaction? = nil,
        sessionId: SessionIdentifier?
    ) -> EventLoopFuture<MongoServerReply> {
        do {
            let request = try BSONEncoder().encode(command)

            return execute(request, namespace: namespace, in: transaction, sessionId: sessionId)
        } catch {
            self.logger.error("Unable to encode MongoDB request. \(error)")
            return eventLoop.makeFailedFuture(error)
        }
    }

    public func execute(
        _ command: Document,
        namespace: MongoNamespace,
        in transaction: MongoTransaction? = nil,
        sessionId: SessionIdentifier? = nil
    ) -> EventLoopFuture<MongoServerReply> {
        let result: EventLoopFuture<MongoServerReply>
        
        if
            let serverHandshake = serverHandshake,
            serverHandshake.maxWireVersion.supportsOpMessage
        {
            result = executeOpMessage(command, namespace: namespace, in: transaction, sessionId: sessionId)
        } else {
            result = executeOpQuery(command, namespace: namespace, in: transaction, sessionId: sessionId)
        }
        
        return result
    }
    
    public func executeOpQuery(
        _ query: inout OpQuery,
        in transaction: MongoTransaction? = nil,
        sessionId: SessionIdentifier? = nil
    ) -> EventLoopFuture<OpReply> {
        query.header.requestId = self.nextRequestId()
        return self.executeMessage(query).flatMapThrowing { reply in
            guard case .reply(let reply) = reply else {
                self.logger.error("Unexpected reply type, expected OpReply")
                throw MongoError(.queryFailure, reason: .invalidReplyType)
            }
            
            return reply
        }
    }
    
    public func executeOpMessage(
        _ query: inout OpMessage,
        in transaction: MongoTransaction? = nil,
        sessionId: SessionIdentifier? = nil
    ) -> EventLoopFuture<OpMessage> {
        query.header.requestId = self.nextRequestId()
        return self.executeMessage(query).flatMapThrowing { reply in
            guard case .message(let message) = reply else {
                self.logger.error("Unexpected reply type, expected OpMessage")
                throw MongoError(.queryFailure, reason: .invalidReplyType)
            }
            
            return message
        }
    }

    fileprivate func executeOpQuery(
        _ command: Document,
        namespace: MongoNamespace,
        in transaction: MongoTransaction? = nil,
        sessionId: SessionIdentifier? = nil
    ) -> EventLoopFuture<MongoServerReply> {
        self.logger.trace("Forming OpQuery")
        
        var command = command
        
        if let id = sessionId?.id {
            self.logger.trace("Session ID \(id)")
            // TODO: This is memory heavy
            command["lsid"]["id"] = id
        }
        
        return self.executeMessage(
            OpQuery(
                query: command,
                requestId: self.nextRequestId(),
                fullCollectionName: namespace.fullCollectionName
            )
        ).recordInterval(to: queryTimer)
    }

    fileprivate func executeOpMessage(
        _ command: Document,
        namespace: MongoNamespace,
        in transaction: MongoTransaction? = nil,
        sessionId: SessionIdentifier? = nil
    ) -> EventLoopFuture<MongoServerReply> {
        self.logger.trace("Forming OpMessage")
        var command = command
        
        command["$db"] = namespace.databaseName
        
        if let id = sessionId?.id {
            self.logger.trace("Session ID \(id)")
            // TODO: This is memory heavy
            command["lsid"]["id"] = id
        }
        
        // TODO: When retrying a write, don't resend transaction messages except commit & abort
        if let transaction = transaction {
            command["txnNumber"] = transaction.number
            command["autocommit"] = transaction.autocommit

            if transaction.startTransaction() {
                command["startTransaction"] = true
                self.logger.info("Starting Transaction with ID: \(transaction.number)")
            }
        }
        
        return self.executeMessage(
            OpMessage(
                body: command,
                requestId: self.nextRequestId()
            )
        ).recordInterval(to: queryTimer)
    }
}

extension EventLoopFuture {
    func recordInterval(to timer: Timer?) -> Self {
        if let timer = timer {
            let start = DispatchTime.now()
            
            self.whenComplete { _ in
                timer.recordInterval(since: start, end: .now())
            }
        }
        
        return self
    }
}
