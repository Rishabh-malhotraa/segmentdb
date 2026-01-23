# Design Doc Template

Use this template when creating design documents for SegmentDB components.

---

## Prompt to Generate Design Docs

After iterating on a file, use this prompt:

```
Create a design document for [FILE_PATH] following the design doc format in docs/DESIGN_DOC_TEMPLATE.md. Document the iterations we went through in this conversation.
```

---

## Template

```markdown
# [Component Name] Design Document

*Last Updated: [DATE]*

Brief description of what this component does.

## Final Design

Show the final code/structure:

``\`\`\```python
# Final implementation
``\`\`\```

**Key points:**
- Bullet points highlighting important aspects

---

## Design Iterations

### Iteration 1: [Short Title]

**Problem:** What issue were we solving?

**Original:**
``\`\`\```python
# Original approach
``\`\`\```

**Final:**
``\`\`\```python
# Final approach
``\`\`\```

**Rationale:**
- Why we made this change
- Benefits of the new approach

---

### Iteration 2: [Short Title]

...repeat for each significant change...

---

## [Optional Sections]

Include as needed:

- **Thread Safety Analysis** — for concurrent code
- **Memory Layout** — for data structures
- **File Format** — for on-disk formats
- **Usage** — code examples

---

## Usage

``\`\`\```python
# Example usage
``\`\`\```
```

---

## Guidelines

1. **Put decisions at the TOP** — readers should see the final design first
2. **Number iterations chronologically** — 1, 2, 3...
3. **Skip trivial changes** — don't document obvious fixes
4. **Include code snippets** — show before/after where helpful
5. **Keep rationale concise** — explain *why*, not *what*
6. **Use tables for comparisons** — pros/cons, options considered
7. **Add diagrams for complex layouts** — ASCII art is fine

---

## Examples

See existing design docs:
- [memtable.md](memtable.md) — Thread safety, memory layout, multiple iterations
- [batching.md](batching.md) — Options comparison, tradeoff analysis
- [sstable.md](sstable.md) — File format specification
