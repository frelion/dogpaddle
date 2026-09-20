use dogpaddle_operation::operation::{AtomicOperation, Operation};

/// The executable linear Operation sequence inside one Station.
pub(crate) struct StationProgram {
    head: Operation,
    tail: Vec<Box<dyn AtomicOperation>>,
}

impl StationProgram {
    pub(crate) fn new(operations: Vec<Operation>) -> Self {
        let mut operations = operations.into_iter();
        let head = operations
            .next()
            .expect("a validated Station program is nonempty");
        let tail = operations
            .map(|operation| match operation {
                Operation::Atomic(operation) => operation,
                Operation::Turn(_) => {
                    unreachable!("a validated Station tail contains only atomic Operations")
                }
            })
            .collect();
        Self { head, tail }
    }

    pub(crate) fn operations_mut(&mut self) -> (&mut Operation, &mut [Box<dyn AtomicOperation>]) {
        (&mut self.head, &mut self.tail)
    }

    #[cfg(test)]
    pub(crate) fn replace_head(&mut self, operation: Operation) {
        self.head = operation;
    }

    #[cfg(test)]
    pub(crate) fn replace_tail(&mut self, tail: Vec<Box<dyn AtomicOperation>>) {
        self.tail = tail;
    }
}
