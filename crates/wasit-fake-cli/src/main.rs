fn main() {
    std::process::exit(wasit_fake_cli::cli::main_with(
        std::env::args().skip(1).collect(),
    ));
}
