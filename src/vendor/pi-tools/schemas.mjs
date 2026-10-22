// SPDX-License-Identifier: MIT
// Copyright (c) 2026 newoostory
import { Type } from 'typebox';
import { Check } from 'typebox/value';
import { failure, ANCHOR_PATTERN, ANCHOR_MAX_LENGTH, FILE_TOOL_PROTOCOL } from './hashline.mjs';
const Path = Type.String({ minLength: 1, maxLength: 4096 });
const Anchor = Type.String({ pattern: ANCHOR_PATTERN, maxLength: ANCHOR_MAX_LENGTH });
export const readSchema = Type.Object({ path: Path,
  offset: Type.Optional(Type.Integer({ minimum: 1 })), limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 2000 })),
  byteOffset: Type.Optional(Type.Integer({ minimum: 0 })), limitBytes: Type.Optional(Type.Integer({ minimum: 1, maximum: 65536 }))
}, { additionalProperties: false });
export const editSchema = Type.Object({ path: Path,
  expectedSha256: Type.String({ pattern: '^[0-9a-f]{64}$' }),
  edits: Type.Array(Type.Object({
    op: Type.String({ enum: ['replace', 'insert_before', 'insert_after'] }), start: Anchor, end: Type.Optional(Anchor),
    newLines: Type.Array(Type.String({ maxLength: 2 * 1024 * 1024 }), { maxItems: 100000 }),
  }, { additionalProperties: false }), { minItems: 1, maxItems: 32 }),
}, { additionalProperties: false });
export const grepSchema = Type.Object({ pattern: Type.String({ minLength: 1, maxLength: 4096 }),
  path: Type.Optional(Path), glob: Type.Optional(Type.String({ minLength: 1, maxLength: 512 })),
  literal: Type.Optional(Type.Boolean()), ignoreCase: Type.Optional(Type.Boolean()),
  context: Type.Optional(Type.Integer({ minimum: 0, maximum: 5 })),
  limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 1000 })),
}, { additionalProperties: false });

// One fixed file-tool contract for every model/profile. This verifies installed
// definitions, not model quality or extra filesystem permissions.
export { FILE_TOOL_PROTOCOL };
export const HASHLINE_TOOL_NAMES = Object.freeze(['read', 'grep', 'edit']);
const fileSchemas = { read: readSchema, grep: grepSchema, edit: editSchema };
export function assertHashlineTools(tools, allowedNames = HASHLINE_TOOL_NAMES) {
  const allowed = new Set(allowedNames), installed = [];
  for (const name of HASHLINE_TOOL_NAMES) {
    const matches = tools.filter(tool => tool.name === name);
    if (matches.length !== (allowed.has(name) ? 1 : 0)) throw failure(`File tool contract mismatch: ${name} registration/permission.`, 'CAPABILITY');
    if (!matches.length) continue;
    if (JSON.stringify(matches[0].parameters) !== JSON.stringify(fileSchemas[name])) {
      throw failure(`File tool contract mismatch: ${name} must use ${FILE_TOOL_PROTOCOL}; no legacy fallback.`, 'CAPABILITY');
    }
    installed.push(name);
  }
  return { protocol: FILE_TOOL_PROTOCOL, tools: installed };
}
export function validate(schema, input) {
  // Bound traversal before Check/stringify, including direct execute callers.
  let budget = 2 * 1024 * 1024, nodes = 0; const seen = new Set();
  const visit = (value, depth) => {
    if (++nodes > 100000 || depth > 8) throw failure('Tool input complexity limit.', 'LIMIT');
    if (typeof value === 'string') {
      if (value.length > budget) throw failure('Tool input exceeds 2 MiB.', 'LIMIT');
      budget -= Buffer.byteLength(JSON.stringify(value));
    } else if (value && typeof value === 'object') {
      if (seen.has(value) || (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype)) throw failure('Tool input must be plain acyclic JSON.');
      seen.add(value); budget -= 2;
      for (const key of Object.keys(value)) { budget--; visit(key, depth + 1); visit(value[key], depth + 1); }
      seen.delete(value);
    } else budget -= 24;
    if (budget < 0) throw failure('Tool input exceeds 2 MiB.', 'LIMIT');
  };
  visit(input, 0);
  if (!Check(schema, input)) throw failure('Invalid tool arguments. Use the current schema; old edit calls require a fresh read, not automatic replay.');
}
