pub enum RespState {
    Idle,
    // ArrayDef,
    // BulkDef,
    BulkData,
    End,
    Error,
}
