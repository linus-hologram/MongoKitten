public enum MongoClientRequest {
    case query(OpQuery)
    case message(OpMessage)
    
    public var requestid: Int32 {
        switch self {
        case .message(let message):
            return message.header.requestId
        case .query(let query):
            return query.header.requestId
        }
    }
    
    public var documents: [Document] {
        switch self {
        case .message(let message):
            var documents = [Document]()
            
            for section in message.sections {
                switch section {
                case .body(let document):
                    documents.append(document)
                case .sequence(let sequence):
                    documents.append(contentsOf: sequence.documents)
                }
            }
            
            return documents
        case .query(let query):
            return [query.query]
        }
    }
}
