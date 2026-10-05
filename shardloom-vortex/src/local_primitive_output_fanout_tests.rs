use super::*;
use std::cell::{Cell, RefCell};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-shared-fanout-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn targets(&self) -> Vec<(PathBuf, Format)> {
        vec![
            (self.0.join("one"), Format::Jsonl),
            (self.0.join("two"), Format::Csv),
        ]
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn fanout_stages_all_outputs_before_publication_and_cleans_owned_staging() {
    let fixture = Fixture::new();
    let targets = fixture.targets();
    let results = write(
        &targets,
        false,
        |_| Ok(()),
        |path, format| {
            assert!(targets.iter().all(|(path, _)| !path.exists()));
            fs::write(path, format.as_str()).unwrap();
            Ok(format)
        },
    )
    .unwrap();
    assert_eq!(results, [Format::Jsonl, Format::Csv]);
    for (path, format) in &targets {
        assert_eq!(fs::read_to_string(path).unwrap(), format.as_str());
    }
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 2);
}

#[test]
fn fanout_failed_adapter_does_not_publish_or_leave_owned_staging() {
    let fixture = Fixture::new();
    let targets = fixture.targets();
    let mut calls = 0;
    let result = write(
        &targets,
        false,
        |_| Ok(()),
        |path, _| {
            calls += 1;
            if calls == 2 {
                return Err(failed("injected adapter denial"));
            }
            fs::write(path, "complete").unwrap();
            Ok(())
        },
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("injected adapter denial")
    );
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
}

#[test]
fn fanout_rejects_existing_duplicate_and_invalid_targets_before_execution() {
    let fixture = Fixture::new();
    let targets = fixture.targets();
    let calls = Cell::new(0);
    let mut adapter = |_: &Path, _: Format| {
        calls.set(calls.get() + 1);
        Ok(())
    };
    fs::write(&targets[0].0, "existing").unwrap();
    assert!(write(&targets, true, |_| Ok(()), &mut adapter).is_err());
    assert_eq!(fs::read_to_string(&targets[0].0).unwrap(), "existing");
    fs::remove_file(&targets[0].0).unwrap();
    assert!(
        write(
            &[targets[0].clone(), targets[0].clone()],
            false,
            |_| Ok(()),
            &mut adapter
        )
        .is_err()
    );
    assert!(
        write(
            &targets,
            false,
            |_| Err(failed("source alias")),
            &mut adapter
        )
        .is_err()
    );
    assert!(write(&[], false, |_| Ok(()), &mut adapter).is_err());
    assert!(
        write(
            &vec![targets[0].clone(); 33],
            false,
            |_| Ok(()),
            &mut adapter
        )
        .is_err()
    );
    assert_eq!(calls.get(), 0);
}

#[test]
fn fanout_late_publication_collision_preserves_complete_and_foreign_outputs() {
    let fixture = Fixture::new();
    let targets = fixture.targets();
    let validations = Cell::new(0);
    let result = write(
        &targets,
        false,
        |_| {
            validations.set(validations.get() + 1);
            if validations.get() == 4 {
                fs::write(&targets[1].0, "concurrent writer").unwrap();
            }
            Ok(())
        },
        |path, _| {
            fs::write(path, "complete").unwrap();
            Ok(())
        },
    );
    let error = result.unwrap_err().to_string();
    assert!(error.contains("previously published complete outputs are preserved"));
    assert!(error.contains(targets[0].0.to_str().unwrap()));
    assert_eq!(fs::read_to_string(&targets[0].0).unwrap(), "complete");
    assert_eq!(
        fs::read_to_string(&targets[1].0).unwrap(),
        "concurrent writer"
    );
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 2);
}

#[test]
fn fanout_does_not_remove_a_replaced_staged_payload() {
    let fixture = Fixture::new();
    let targets = fixture.targets();
    let staged = RefCell::new(Vec::<PathBuf>::new());
    let validations = Cell::new(0);
    let result = write(
        &targets,
        false,
        |_| {
            validations.set(validations.get() + 1);
            if validations.get() == 4 {
                let staged = staged.borrow();
                fs::remove_file(&staged[1]).unwrap();
                fs::write(&staged[1], "foreign payload").unwrap();
            }
            Ok(())
        },
        |path, _| {
            fs::write(path, "complete").unwrap();
            staged.borrow_mut().push(path.to_path_buf());
            Ok(())
        },
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("temporary identity changed")
    );
    assert_eq!(fs::read_to_string(&targets[0].0).unwrap(), "complete");
    assert!(!targets[1].0.exists());
    assert_eq!(
        fs::read_to_string(&staged.borrow()[1]).unwrap(),
        "foreign payload"
    );
}
