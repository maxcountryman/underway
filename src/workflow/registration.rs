use std::marker::PhantomData;

use crate::activity::Activity;

/// Marker type representing no registered activities.
pub struct NoActivities;

/// Type-level set node for registered activities.
pub struct Registered<Head, Tail>(pub(crate) PhantomData<(Head, Tail)>);

/// Type-level index for the head of a set.
pub struct Here;

/// Type-level index for an element in the tail of a set.
pub struct There<T>(pub(crate) PhantomData<T>);

mod sealed {
    pub trait ActivitySet {}
}

/// Type-level set of activities registered on a workflow builder.
pub trait ActivitySet: sealed::ActivitySet + 'static {}

impl<T> ActivitySet for T where T: sealed::ActivitySet + 'static {}

impl sealed::ActivitySet for NoActivities {}

impl<Head, Tail> sealed::ActivitySet for Registered<Head, Tail>
where
    Head: Activity,
    Tail: ActivitySet,
{
}

mod private {
    use super::{Activity, Here, Registered, There};

    pub trait Contains<A: Activity, Idx> {}

    impl<A, Tail> Contains<A, Here> for Registered<A, Tail> where A: Activity {}

    impl<A, Head, Tail, Idx> Contains<A, There<Idx>> for Registered<Head, Tail>
    where
        A: Activity,
        Head: Activity,
        Tail: Contains<A, Idx>,
    {
    }
}

/// Marker trait indicating `A` is present in an activity set.
pub trait Contains<A: Activity, Idx>: ActivitySet {}

impl<Set, A, Idx> Contains<A, Idx> for Set
where
    Set: ActivitySet + private::Contains<A, Idx>,
    A: Activity,
{
}
