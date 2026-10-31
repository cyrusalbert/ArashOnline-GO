# arashonline

Collects v2ray configs from the subscription links in `sourcelist.txt`, tests them with Xray-core, and publishes two ready-to-import subscription files. Refreshes every 12 hours via GitHub Actions.

## Outputs

| File | Content |
|---|---|
| `arashonline.txt` | All configs that passed, sorted by ping (lowest first) |
| `arashonline-highspeed.txt` | Top 50 of those by download speed |

Remark format: `1 | 🇩🇪 | 1.2.3.4:443 | 87ms | 23.4Mbps` (rank by ping | exit country | address:port | ping | speed).

Subscription URL for your client:
`https://raw.githubusercontent.com/<USER>/<REPO>/main/arashonline.txt`

## Pipeline

1. Fetch every URL in `sourcelist.txt` (plain or base64). Supports vmess, vless (incl. Reality), trojan and shadowsocks. Duplicates are removed.
2. Alive test through `http://www.gstatic.com/generate_204`. Ping must be 3000 ms or less.
3. Download speed test through each alive config. Only configs at 50 Mbps or more are kept.
4. Exit country is looked up through the proxy and turned into a flag emoji.
5. Remarks are rewritten and both files are written. If nothing survives, the old files are left untouched.

## Setup

1. Create a repo, upload these files, keep `.github/workflows/update.yml` in that exact path.
2. Put your subscription URLs in `sourcelist.txt`.
3. Actions tab > "Update configs" > Run workflow (the schedule handles the rest).

## Run locally

Needs Go 1.22+ and the [Xray-core](https://github.com/XTLS/Xray-core/releases) binary.

```
go run . -xray /path/to/xray -max-speed 0 -min-speed 50
```

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `-sources` | `sourcelist.txt` | Subscription list |
| `-max-ping` | `3000` | Max ping in ms |
| `-max-speed` | `50` | Drop configs faster than this many Mbps (0 = off) |
| `-min-speed` | `0` | Drop configs slower than this many Mbps (0 = off) |
| `-top` | `50` | Size of the high-speed file |
| `-workers` | `30` | Parallel ping tests |
| `-speed-workers` | `4` | Parallel speed tests |
| `-speed-secs` | `8` | Max seconds of download per config |
| `-ping-url` | gstatic generate_204 | Must answer HTTP 204 |
| `-speed-url` | Cloudflare `__down` | Any large file URL |

The table shows the program's built-in defaults. The workflow runs with `-max-speed 0 -min-speed 50`, so only configs of 50 Mbps or more are kept. If none reach 50 Mbps, the run fails and the previous files stay as they are; lower `-min-speed` in `update.yml` if that happens.

## Notes

- Tests run on GitHub's servers, so ping and speed reflect that location, not your own ISP.
- `arashonline-highspeed.txt` reuses the same lines, so the number in each remark is still the ping rank.
