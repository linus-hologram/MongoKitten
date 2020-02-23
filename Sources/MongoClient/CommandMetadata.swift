import Foundation

public struct CommandMetadata: Encodable {
    private let file: StaticString
    private let line: UInt
    
    public init(file: StaticString = #file, line: UInt = #line) {
        self.file = file
        self.line = line
    }
    
    private enum CodingKeys: String, CodingKey {
        case file, line
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        let data = Data(bytes: file.utf8Start, count: file.utf8CodeUnitCount)
        let string = String(data: data, encoding: .utf8) ?? ""
        
        try container.encode(string, forKey: .file)
        try container.encode(line, forKey: .line)
    }
}
