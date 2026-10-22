# Third-party notices

Jsubagent is the public continuation of the maintainer's local `pi-agent` / `pi-subagents` extension. It preserves the internal `pi-agent` protocol and state namespace. The four local file-tool modules in `src/vendor/pi-tools` are included in this project's MIT license. They implement bounded file I/O, hashline edits, schemas, and ripgrep integration; the former private `pi-tools` package is not required.

Pi's SDK packages and TypeBox are host-provided external dependencies, with development versions recorded in `package-lock.json`. Their source and transitive dependencies retain their respective licenses. The optional private `pi-web-fusion` and `pi-super` packages are not redistributed here.

The Pi SDK is distributed under the MIT license. Its notice is retained below for SDK-derived integration material:

## Pi

Source: <https://github.com/earendil-works/pi>

MIT License

Copyright (c) 2025 Mario Zechner

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
