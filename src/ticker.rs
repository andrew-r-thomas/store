pub trait Ticker<Ctx>: Sink {
    type Output;

    fn tick<S: Sink<Input = Self::Output>>(&mut self, ctx: Ctx, sink: &mut S);
}

pub trait Sink {
    type Input: ?Sized;
    fn push(&mut self, input: &Self::Input);
}
