# Required salt configuration test failure

## Failed case

After removing the repository-known host identity salt default, three interval-validation tests
failed with `HOST_ID_SALT must not be empty` before reaching the interval value they intended to
exercise.

## Why the previous method failed

The tests relied on all unrelated configuration using defaults. Making the private salt required was
an intentional secure-default change, so that assumption was no longer valid.

## Improved method

Each interval test now supplies an explicit test-only placeholder salt before setting the invalid
interval. Missing, short, and unchanged example salts remain covered by separate negative tests.
This makes each test's prerequisite configuration explicit without weakening production validation.
