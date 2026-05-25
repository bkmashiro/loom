package sandbox

// EphemeralSandbox returns a Config with a single in-memory mount at "/".
func EphemeralSandbox() Config {
	return Config{Mounts: []Mount{{Guest: "/", Mode: Ephemeral}}}
}

// ReadOnlySandbox returns a Config with a single read-only host mount at "/".
func ReadOnlySandbox(hostDir string) Config {
	return Config{Mounts: []Mount{{Guest: "/", Host: hostDir, Mode: ReadOnly}}}
}

// ReadWriteSandbox returns a Config with a single read-write host mount at "/".
func ReadWriteSandbox(hostDir string) Config {
	return Config{Mounts: []Mount{{Guest: "/", Host: hostDir, Mode: ReadWrite}}}
}

// PersistentSandbox returns a Config with a single persistent host mount at "/".
// The host directory is created if it does not exist.
func PersistentSandbox(hostDir string) Config {
	return Config{Mounts: []Mount{{Guest: "/", Host: hostDir, Mode: Persistent}}}
}
