# FSK441+

High-performance FSK441 meteor scatter transceiver for macOS, Linux and Windows.

Developed by Roger Banks GW4WND / The DX Shop

## Features

- Real-time FSK441 decode with waterfall display
- Soft-bit accumulator — merges weak ping fragments for clean decodes
- Two-arm adaptive threshold, data-validated against real QSOs
- Background threshold optimiser — learns your noise floor automatically  
- QSO mode with automatic constraint-guided decoding
- CAT control via hamlib (rigctld) with inline KHz frequency entry
- ADIF logging to `~/.fsk441/fsk441.adi`
- QSO transcript export (clipboard + file)
- SQLite session database with WAL journaling

## Build from source

```bash
git clone https://github.com/Nythbran23/FSK441-PLUS.git
cd FSK441-PLUS
cargo build --bin fsk441 --release
./target/release/fsk441
```

### Linux build dependencies
```bash
sudo apt-get install libasound2-dev libgtk-3-dev \
  libxcb-render0-dev libxcb-shape0-dev libxcb-xfixes0-dev \
  libxkbcommon-dev pkg-config
```

## Geographic callsign validation

Place `cty.dat` (from WSJT-X or MSHV) alongside the binary, or specify
the path in Settings. Enables geographic filtering of implausible callsigns.

## Data files

All data stored in `~/.fsk441/`:
- `fsk441.db` — SQLite session database (pings, threshold history)
- `fsk441.adi` — ADIF log (compatible with WSJT-X, HAMRS, LoTW)
- `fsk441.cfg` — settings
- `transcripts/` — QSO transcript exports

## GitHub Actions secrets required for notarized macOS builds

| Secret | Description |
|--------|-------------|
| `MACOS_CERTIFICATE` | Base64-encoded Developer ID certificate (.p12) |
| `MACOS_CERTIFICATE_PWD` | Certificate password |
| `KEYCHAIN_PASSWORD` | Temporary keychain password |
| `MACOS_IDENTITY` | Signing identity string |
| `APPLE_ID` | Apple ID for notarization |
| `APPLE_PASSWORD` | App-specific password |
| `APPLE_TEAM_ID` | Apple Developer Team ID |

## License

MIT — © 2026 Roger Banks GW4WND / The DX Shop
