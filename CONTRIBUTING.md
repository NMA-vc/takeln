# Contributing to `takeln`

Thank you for your interest in contributing to `takeln`! We welcome contributions from the community and appreciate your help in making this project better.

## Reporting Bugs

If you find a bug, please open an issue on GitHub:  
[https://github.com/NMA-vc/takeln/issues](https://github.com/NMA-vc/takeln/issues)

Please include as much detail as possible, including steps to reproduce the issue, expected behavior, and actual behavior.

## Requesting Features

Have an idea for a new feature? We'd love to hear it! Please open an issue on GitHub:  
[https://github.com/NMA-vc/takeln/issues](https://github.com/NMA-vc/takeln/issues)

Describe the feature clearly and provide any relevant context or examples.

## Development Setup

To get started with development:

1. Clone the repository:

   ```bash
   git clone https://github.com/NMA-vc/takeln.git
   cd takeln
   ```

2. Build the project:

   ```bash
   cargo build
   ```

3. Run tests:

   ```bash
   cargo test
   ```

4. Run tests with optional features (e.g., `sqlite`):

   ```bash
   cargo test --features sqlite
   ```

## Code Style

We use `rustfmt` and `clippy` to enforce consistent code style and catch common issues.

- Run `cargo fmt` to format your code. A `rustfmt.toml` configuration file exists in the repository root.

- Run `cargo clippy --all-features` to lint your code before committing.

## Pull Request Workflow

1. Fork the repository.
2. Create a new branch for your changes:

   ```bash
   git checkout -b feature-or-fix/your-branch-name
   ```

3. Make your changes, and commit them with clear, descriptive commit messages.

4. Push your branch to your fork:

   ```bash
   git push origin feature-or-fix/your-branch-name
   ```

5. Open a pull request on GitHub.

All pull requests must pass continuous integration (CI) checks, including `cargo fmt`, `cargo clippy --all-features`, and `cargo test --all-features`, before they can be merged.

## License

By contributing to `takeln`, you agree that your contributions will be licensed under the [Apache-2.0 License](https://github.com/NMA-vc/takeln/blob/main/LICENSE). Please see the `LICENSE` file for more details.

Thank you again for your contributions!
