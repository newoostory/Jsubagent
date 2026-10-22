import { LIMITS, encodeJson, fail } from './limits.mjs';

export class FrameDecoder {
  constructor(maxBytes = LIMITS.frameBytes) { this.maxBytes = maxBytes; this.header = Buffer.alloc(4); this.headerUsed = 0; this.body = null; this.used = 0; }
  *push(chunk) {
    let pos = 0;
    while (pos < chunk.length) {
      if (!this.body) {
        const n = Math.min(4 - this.headerUsed, chunk.length - pos);
        chunk.copy(this.header, this.headerUsed, pos, pos + n); this.headerUsed += n; pos += n;
        if (this.headerUsed !== 4) continue;
        const size = this.header.readUInt32BE();
        if (!size || size > this.maxBytes) throw fail('Oversized/empty IPC frame', 'PROTOCOL');
        this.body = Buffer.allocUnsafe(size); this.used = 0;
      }
      const n = Math.min(this.body.length - this.used, chunk.length - pos);
      chunk.copy(this.body, this.used, pos, pos + n); this.used += n; pos += n;
      if (this.used === this.body.length) {
        const raw = this.body; this.body = null; this.headerUsed = 0; this.used = 0;
        let value;
        try { value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(raw)); }
        catch { throw fail('Invalid IPC JSON/UTF-8', 'PROTOCOL'); }
        encodeJson(value, this.maxBytes); // depth/shape limit, bounded input
        if (Object.hasOwn(value, 'v') || Object.hasOwn(value, 'version')) throw fail('Incompatible IPC frame; restart both peers together', 'PROTOCOL');
        yield value;
      }
    }
  }
  end() { if (this.body || this.headerUsed) throw fail('Incomplete IPC frame', 'PROTOCOL'); }
}
export async function* readFrames(readable, maxBytes = LIMITS.frameBytes) {
  const decoder = new FrameDecoder(maxBytes);
  for await (const chunk of readable) yield* decoder.push(chunk);
  decoder.end();
}
export class FrameWriter {
  constructor(stream, { frameBytes = LIMITS.frameBytes, queueBytes = LIMITS.queueBytes } = {}) {
    this.stream = stream; this.frameBytes = frameBytes; this.maxQueue = queueBytes; this.queued = 0; this.tail = Promise.resolve(); this.error = null;
    this.onError = error => { this.error = error; };
    stream.on('error', this.onError);
  }
  send(value) {
    if (this.error || this.stream.destroyed) return Promise.reject(this.error ?? fail('IPC closed', 'CLOSED'));
    let body;
    try { body = encodeJson(value, this.frameBytes); } catch (e) { return Promise.reject(e); }
    if (this.queued + body.length + 4 > this.maxQueue) return Promise.reject(fail('IPC queue full', 'BUSY'));
    this.queued += body.length + 4;
    const header = Buffer.allocUnsafe(4); header.writeUInt32BE(body.length);
    const work = this.tail.then(async () => {
      if (this.error) throw this.error;
      // Await callbacks as well as respecting serialization: no unbounded writable queue.
      await new Promise((resolve, reject) => this.stream.write(header, e => e ? reject(e) : resolve()));
      await new Promise((resolve, reject) => this.stream.write(body, e => e ? reject(e) : resolve()));
    }).finally(() => { this.queued -= body.length + 4; });
    this.tail = work.catch(() => {});
    return work;
  }
  async close() { await this.tail; this.stream.end(); }
}
