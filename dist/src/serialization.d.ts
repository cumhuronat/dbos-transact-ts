import type { JSONValue } from 'superjson/dist/types';
import { WorkflowSerializationFormat } from './workflow';
export { type JSONValue };
/**
 * Generic serializer interface for DBOS.
 * Implementations must be able to serialize any value to a string and deserialize it back.
 *
 * Both `stringify` and `parse` may return synchronously or asynchronously, so async-only
 * libraries can be used to encrypt/decrypt persisted values.
 */
export interface DBOSSerializer {
    /**
     * Return a name for the serialization format
     */
    name: () => string;
    /**
     * Serialize a value to a string.
     * @param value - The value to serialize
     * @returns The serialized string representation, or a Promise resolving to it
     */
    stringify(value: unknown): string | Promise<string>;
    /**
     * Deserialize a string back to a value.
     * @param text - A serialized string (potentially null or undefined)
     * @returns The deserialized value (or null if the input was null/undefined).
     *          Implementations may return a Promise; DBOS awaits the result.
     */
    parse(text: string | null | undefined): unknown;
}
export type SerializationRecipe<T, S extends JSONValue> = {
    name: string;
    isApplicable: (v: unknown) => v is T;
    serialize: (v: T) => S;
    deserialize: (s: S) => T;
};
export declare function registerSerializationRecipe<T, S extends JSONValue>(r: SerializationRecipe<T, S>): void;
export declare function DBOSReplacer(this: any, key: string, value: unknown): unknown;
export declare function DBOSReviver(_key: string, value: unknown): unknown;
export declare const DBOSJSONLegacy: {
    name: () => string;
    parse: (text: string | null) => any;
    stringify: (value: unknown) => string | undefined;
};
export declare const SERIALIZER_MARKER_KEY = "__dbos_serializer";
export declare const SERIALIZER_MARKER_VALUE = "superjson";
/**
 * DBOSJSON with SuperJSON support for richer type serialization.
 *
 * Backwards compatible - can deserialize both old DBOSJSON format and new SuperJSON format.
 * New serialization uses SuperJSON to handle Sets, Maps, undefined, RegExp, circular refs, etc.
 */
export declare const DBOSJSON: {
    name: () => string;
    parse: (text: string | null | undefined) => unknown;
    stringify: (value: unknown) => string;
};
/**
 * DBOS Portable JSON serializer,
 *   should be something that can be implemented in any language.
 */
export declare const DBOSPortableJSON: {
    name: () => string;
    parse: (text: string | null) => unknown;
    stringify: (value: unknown) => string;
};
type PathToMember = Array<string | number | symbol>;
/**
 * Roundtrips `value` through serialization.  This doesn't preserve functions by default.
 *   So then, we recursively attach function stubs that throw clear errors, for any
 *   functions present on the original (own props + prototype methods) that
 *   aren't present as functions on the deserialized object.
 * The return is both the deserialized object and its serialized string.
 */
export declare function serializeFunctionInputOutput<T>(value: T, path: PathToMember | undefined, serializer: DBOSSerializer, serializationType?: WorkflowSerializationFormat): Promise<{
    deserialized: T;
    stringified: string;
    sername: string;
}>;
export declare function serializeFunctionInputOutputWithSerializer<T>(value: T, path: PathToMember | undefined, serializer: DBOSSerializer, serialization: string | null): Promise<{
    deserialized: T;
    stringified: string;
    sername: string;
}>;
export declare function deserializeValue(serializedValue: string | null, serialization: string | null, serializer: DBOSSerializer): Promise<unknown>;
export declare function deserializePositionalArgs(serializedValue: string | null, serialization: string | null, serializer: DBOSSerializer): Promise<unknown[]>;
export declare function deserializeResError(serializedValue: string | null, serialization: string | null, serializer: DBOSSerializer): Promise<Error>;
export declare function safeParse(serializer: DBOSSerializer, val: string, serialization: string | null): Promise<unknown>;
export declare function safeParsePositionalArgs(serializer: DBOSSerializer, val: string | null, serialization: string | null): Promise<string | unknown[] | null>;
export declare function safeParseError(serializer: DBOSSerializer, val: string, serialization: string | null): Promise<Error>;
export declare function serializeValue(value: unknown, serializer: DBOSSerializer, serializationFormat: WorkflowSerializationFormat): Promise<{
    serializedValue: string | null;
    serialization: string | null;
}>;
export declare function serializeArgs(positionalArgs: unknown[] | undefined, namedArgs: {
    [key: string]: unknown;
} | undefined, serializer: DBOSSerializer, serializationFormat: WorkflowSerializationFormat): Promise<{
    serializedValue: string | null;
    serialization: string | null;
}>;
export declare function serializeResError(err: Error, serializer: DBOSSerializer, serializationType: WorkflowSerializationFormat): Promise<{
    serializedValue: string | null;
    serialization: string | null;
}>;
export declare function serializeResErrorWithSerializer(err: Error, serializer: DBOSSerializer, serialization: string | null): Promise<{
    serializedValue: string | null;
    serialization: string | null;
}>;
//# sourceMappingURL=serialization.d.ts.map