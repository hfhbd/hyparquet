import {ConvertedType, FieldRepetitionType, SchemaElement, SchemaTree} from "./types.js";

/**
 * Build a tree from the schema elements.
 *
 * @param {SchemaElement[]} schema
 * @param {number} rootIndex index of the root element
 * @param {string[]} path path to the element
 * @returns {SchemaTree} tree of schema elements
 */
function schemaTree(schema: SchemaElement[], rootIndex: number, path: string[]): SchemaTree {
  const element = schema[rootIndex]
  const children = []
  let count = 1

  // Read the specified number of children
  if (element.num_children) {
    while (children.length < element.num_children) {
      const childElement = schema[rootIndex + count]
      const child = schemaTree(schema, rootIndex + count, [...path, childElement.name])
      count += child.count
      children.push(child)
    }
  }

  return { count, element, children, path }
}

/**
 * Get schema elements from the root to the given element name.
 *
 * @param {SchemaElement[]} schema
 * @param {string[]} name path to the element
 * @returns {SchemaTree[]} list of schema elements
 */
export function getSchemaPath(schema: SchemaElement[], name: string[]): SchemaTree[] {
  let tree = schemaTree(schema, 0, [])
  const path = [tree]
  for (const part of name) {
    const child = tree.children.find(child => child.element.name === part)
    if (!child) throw new Error(`parquet schema element not found: ${name}`)
    path.push(child)
    tree = child
  }
  return path
}

/**
 * Get the max repetition level for a given schema path.
 *
 * @param {SchemaTree[]} schemaPath
 * @returns {number} max repetition level
 */
export function getMaxRepetitionLevel(schemaPath: SchemaTree[]): number {
  let maxLevel = 0
  for (const { element } of schemaPath) {
    if (element.repetition_type === FieldRepetitionType.REPEATED) {
      maxLevel++
    }
  }
  return maxLevel
}

/**
 * Get the max definition level for a given schema path.
 *
 * @param {SchemaTree[]} schemaPath
 * @returns {number} max definition level
 */
export function getMaxDefinitionLevel(schemaPath: SchemaTree[]): number {
  let maxLevel = 0
  for (const { element } of schemaPath.slice(1)) {
    if (element.repetition_type !== FieldRepetitionType.REQUIRED) {
      maxLevel++
    }
  }
  return maxLevel
}

/**
 * Check if a column is list-like.
 *
 * @param {SchemaTree} schema
 * @returns {boolean} true if list-like
 */
export function isListLike(schema: SchemaTree): boolean {
  if (!schema) return false
  if (schema.element.converted_type !== ConvertedType.LIST) return false
  if (schema.children.length > 1) return false

  const firstChild = schema.children[0]
  if (firstChild.children.length > 1) return false
  return firstChild.element.repetition_type === FieldRepetitionType.REPEATED;


}

/**
 * Check if a column is map-like.
 *
 * @param {SchemaTree} schema
 * @returns {boolean} true if map-like
 */
export function isMapLike(schema: SchemaTree): boolean {
  if (!schema) return false
  if (schema.element.converted_type !== ConvertedType.MAP) return false
  if (schema.children.length > 1) return false

  const firstChild = schema.children[0]
  if (firstChild.children.length !== 2) return false
  if (firstChild.element.repetition_type !== FieldRepetitionType.REPEATED) return false

  const keyChild = firstChild.children.find(child => child.element.name === 'key')
  if (keyChild?.element.repetition_type === FieldRepetitionType.REPEATED) return false

  const valueChild = firstChild.children.find(child => child.element.name === 'value')
  return valueChild?.element.repetition_type !== FieldRepetitionType.REPEATED;


}

/**
 * Returns true if a column is non-nested.
 *
 * @param {SchemaTree[]} schemaPath
 * @returns {boolean}
 */
export function isFlatColumn(schemaPath: SchemaTree[]): boolean {
  if (schemaPath.length !== 2) return false
  const column = schemaPath[1]
  if (column.element.repetition_type === FieldRepetitionType.REPEATED) return false
  return !column.children.length;

}
