use super::*;

#[test]
fn the_attribute_boost_is_x_over_x_plus_the_midpoint() {
    let close = |a: f64, b: f64| assert!((a - b).abs() < 1e-12, "{a} != {b}");
    close(attribute(0.0, 10.0), 0.0);
    close(attribute(10.0, 10.0), 0.5);
    close(attribute(30.0, 10.0), 0.75);
    close(attribute(1e300, 1e-300), 1.0);
    // A negative value counts as 0, and a number that is no number as well.
    close(attribute(-5.0, 10.0), 0.0);
    close(attribute(f64::NEG_INFINITY, 10.0), 0.0);
    close(attribute(f64::NAN, 10.0), 0.0);
    close(attribute(f64::INFINITY, 10.0), 1.0);
    // Neither a sum of two large numbers nor a tiny ratio goes wrong.
    close(attribute(1e308, 1e308), 0.5);
    close(attribute(f64::MAX, f64::MAX), 0.5);
    let tiny = attribute(1e-10, 1e300);
    assert!(tiny > 0.0 && tiny < 1e-300, "{tiny}");
}

#[test]
fn the_recency_boost_is_the_midpoint_over_the_midpoint_plus_the_distance() {
    let close = |a: f64, b: f64| assert!((a - b).abs() < 1e-12, "{a} != {b}");
    close(recency(0.0, 3.0), 1.0);
    close(recency(3.0, 3.0), 0.5);
    close(recency(9.0, 3.0), 0.25);
    // A document far from the origin gets next to nothing, never a negative.
    assert!((0.0..1e-9).contains(&recency(1e15, 3.0)));
}
