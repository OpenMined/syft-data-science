import * as $protobuf from "protobufjs";
import Long = require("long");
/** Properties of an Addition. */
export interface IAddition {

    /** Addition a */
    a?: (number|null);

    /** Addition b */
    b?: (number|null);
}

/** Represents an Addition. */
export class Addition implements IAddition {

    /**
     * Constructs a new Addition.
     * @param [properties] Properties to set
     */
    constructor(properties?: IAddition);

    /** Addition a. */
    public a: number;

    /** Addition b. */
    public b: number;

    /**
     * Creates a new Addition instance using the specified properties.
     * @param [properties] Properties to set
     * @returns Addition instance
     */
    public static create(properties?: IAddition): Addition;

    /**
     * Encodes the specified Addition message. Does not implicitly {@link Addition.verify|verify} messages.
     * @param message Addition message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IAddition, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified Addition message, length delimited. Does not implicitly {@link Addition.verify|verify} messages.
     * @param message Addition message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IAddition, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes an Addition message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns Addition
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): Addition;

    /**
     * Decodes an Addition message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns Addition
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): Addition;

    /**
     * Verifies an Addition message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates an Addition message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns Addition
     */
    public static fromObject(object: { [k: string]: any }): Addition;

    /**
     * Creates a plain object from an Addition message. Also converts values to other types if specified.
     * @param message Addition
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: Addition, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this Addition to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };

    /**
     * Gets the default type url for Addition
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
}
