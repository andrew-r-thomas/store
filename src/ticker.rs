pub trait Ticker {
    type Input;
    type Output;

    fn tick(&mut self);
}
