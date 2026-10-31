use super::*;

#[test]
fn ordered_input_check_is_linear_stable_and_keeps_heap_results_for_every_shape() {
    for length in [0, 1, 2, 3, 31, 1024, 4097] {
        let ordered = (0..length).map(|row| (row / 3, row)).collect::<Vec<_>>();
        let mut reversed = ordered.clone();
        reversed.reverse();
        let mut inversion = ordered.clone();
        if length > 1 {
            inversion.swap(length / 2, length / 2 - 1);
        }
        let runs = (0..length).map(|row| ((row / 17) % 5, row)).collect();
        let scrambled = (0..length).map(|row| (row * 31 % 97, row)).collect();
        for input in [ordered.clone(), reversed, inversion, runs, scrambled] {
            let mut expected = input.clone();
            expected.sort_unstable();
            let mut reference = input.clone();
            let mut heap_calls = 0;
            sort(&mut reference, &CancellationToken::default(), |a, b| {
                heap_calls += 1;
                Ok(a.cmp(&b))
            })
            .unwrap();
            let mut actual = input.clone();
            let mut calls = 0;
            sort_with_order_check(&mut actual, &CancellationToken::default(), true, |a, b| {
                calls += 1;
                Ok(a.cmp(&b))
            })
            .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(actual, reference);
            if let Some(first) = input.windows(2).position(|pair| pair[0] > pair[1]) {
                assert_eq!(calls, first + 1 + heap_calls);
            } else {
                assert_eq!(calls, length.saturating_sub(1));
                assert_eq!(actual, input);
                if length > 3 {
                    assert!(calls < heap_calls);
                }
            }
        }
    }
}

#[test]
fn ordered_input_decline_preserves_exact_original_comparison_and_error_trace() {
    for failure in [None, Some(1), Some(2), Some(10), Some(137)] {
        let input = (0..257).map(|row| (row % 17, row)).collect::<Vec<_>>();
        let mut original = input.clone();
        let mut original_trace = Vec::new();
        let original_result = sort(&mut original, &CancellationToken::default(), |a, b| {
            original_trace.push((a, b));
            if failure == Some(original_trace.len()) {
                Err(super::super::native_relational_batch::failed(
                    "declared comparison error",
                ))
            } else {
                Ok(a.cmp(&b))
            }
        });
        let mut actual = input;
        let mut actual_trace = Vec::new();
        let actual_result =
            sort_with_order_check(&mut actual, &CancellationToken::default(), false, |a, b| {
                actual_trace.push((a, b));
                if failure == Some(actual_trace.len()) {
                    Err(super::super::native_relational_batch::failed(
                        "declared comparison error",
                    ))
                } else {
                    Ok(a.cmp(&b))
                }
            });
        assert_eq!(actual_trace, original_trace);
        assert_eq!(actual, original);
        assert_eq!(
            actual_result.map_err(|error| error.to_string()),
            original_result.map_err(|error| error.to_string())
        );
    }
}

#[test]
fn ordered_input_check_polls_cancellation_on_scan_heap_and_empty_boundaries() {
    for reverse in [false, true] {
        for admitted in [false, true] {
            let mut rows = (0..4097).collect::<Vec<_>>();
            if reverse {
                rows.reverse();
            }
            let token = CancellationToken::default();
            let mut calls = 0;
            let result = sort_with_order_check(&mut rows, &token, admitted, |a, b| {
                calls += 1;
                token.cancel();
                Ok(a.cmp(&b))
            });
            assert!(result.is_err());
            assert!((1..=1024).contains(&calls));
        }
    }
    for length in [0, 1, 2] {
        let token = CancellationToken::default();
        token.cancel();
        let mut rows = (0..length).collect::<Vec<_>>();
        for admitted in [false, true] {
            assert!(
                sort_with_order_check(&mut rows, &token, admitted, |_, _| {
                    panic!("already cancelled")
                })
                .is_err()
            );
        }
    }
    let token = CancellationToken::default();
    assert!(
        sort_with_order_check(&mut [0, 1], &token, true, |a, b| {
            token.cancel();
            Ok(a.cmp(&b))
        })
        .is_err()
    );
}
