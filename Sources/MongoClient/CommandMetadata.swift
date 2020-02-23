public struct CommandMetadata: Codable {
    private let file: String
    private let line: UInt
    
    public init(file: String = #file, line: UInt = #line) {
        self.file = file
        self.line = line
    }
}
