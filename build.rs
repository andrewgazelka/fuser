fn main() {
    // Register rustc cfg for switching between mount implementations.
    // When fuser MSRV is updated to v1.77 or above, we should switch from 'cargo:' to 'cargo::' syntax.
    println!(
        "cargo:rustc-check-cfg=cfg(fuser_mount_impl, values(\"pure-rust\", \"libfuse2\", \"libfuse3\", \"none\"))"
    );

    let target_os =
        std::env::var("CARGO_CFG_TARGET_OS").expect("CARGO_CFG_TARGET_OS should be set");

    if matches!(
        target_os.as_str(),
        "linux" | "freebsd" | "dragonfly" | "openbsd" | "netbsd"
    ) && cfg!(not(feature = "libfuse"))
    {
        println!("cargo:rustc-cfg=fuser_mount_impl=\"pure-rust\"");
    } else if target_os == "macos" {
        // On macOS, try to find macFUSE but don't fail if it's missing
        // (useful for development/type-checking without macFUSE installed)
        if pkg_config::Config::new()
            .atleast_version("2.6.0")
            .probe("fuse") // for macFUSE 4.x
            .map_err(|e| eprintln!("Warning: macFUSE not found: {e}"))
            .is_ok()
        {
            println!("cargo:rustc-cfg=fuser_mount_impl=\"libfuse2\"");
            println!("cargo:rustc-cfg=feature=\"macfuse-4-compat\"");
        } else {
            eprintln!("Warning: Building without macFUSE support - mount operations will not work");
            println!("cargo:rustc-cfg=fuser_mount_impl=\"none\"");
        }
    } else {
        // First try to link with libfuse3
        if pkg_config::Config::new()
            .atleast_version("3.0.0")
            .probe("fuse3")
            .map_err(|e| eprintln!("{e}"))
            .is_ok()
        {
            println!("cargo:rustc-cfg=fuser_mount_impl=\"libfuse3\"");
        } else {
            // Fallback to libfuse
            pkg_config::Config::new()
                .atleast_version("2.6.0")
                .probe("fuse")
                .map_err(|e| eprintln!("{e}"))
                .unwrap();
            println!("cargo:rustc-cfg=fuser_mount_impl=\"libfuse2\"");
        }
    }
}
