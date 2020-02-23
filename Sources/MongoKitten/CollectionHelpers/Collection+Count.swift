import MongoCore
import MongoClient
import NIO

extension MongoCollection {
    public func count(_ query: Document? = nil, file: StaticString = #file, line: UInt = #line) -> EventLoopFuture<Int> {
        guard transaction == nil else {
            return makeTransactionError()
        }
        
        return pool.next(for: .basic).flatMap { connection in
            return connection.executeCodable(
                CountCommand(on: self.name, where: query),
                namespace: self.database.commandNamespace,
                sessionId: self.sessionId ?? connection.implicitSessionId,
                metadata: CommandMetadata(file: file, line: line)
            )
        }.decode(CountReply.self).map { $0.count }._mongoHop(to: hoppedEventLoop)
    }
    
    public func count<Query: MongoKittenQuery>(_ query: Query? = nil, file: StaticString = #file, line: UInt = #line) -> EventLoopFuture<Int> {
        return count(query?.makeDocument(), file: file, line: line)
    }
}
