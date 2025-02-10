/*eslint-disable block-scoped-var, id-length, no-control-regex, no-magic-numbers, no-prototype-builtins, no-redeclare, no-shadow, no-var, sort-vars*/
"use strict";

var $protobuf = require("protobufjs/minimal");

// Common aliases
var $Reader = $protobuf.Reader, $Writer = $protobuf.Writer, $util = $protobuf.util;

// Exported root namespace
var $root = $protobuf.roots["default"] || ($protobuf.roots["default"] = {});

$root.Addition = (function() {

    /**
     * Properties of an Addition.
     * @exports IAddition
     * @interface IAddition
     * @property {number|null} [a] Addition a
     * @property {number|null} [b] Addition b
     */

    /**
     * Constructs a new Addition.
     * @exports Addition
     * @classdesc Represents an Addition.
     * @implements IAddition
     * @constructor
     * @param {IAddition=} [properties] Properties to set
     */
    function Addition(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * Addition a.
     * @member {number} a
     * @memberof Addition
     * @instance
     */
    Addition.prototype.a = 0;

    /**
     * Addition b.
     * @member {number} b
     * @memberof Addition
     * @instance
     */
    Addition.prototype.b = 0;

    /**
     * Creates a new Addition instance using the specified properties.
     * @function create
     * @memberof Addition
     * @static
     * @param {IAddition=} [properties] Properties to set
     * @returns {Addition} Addition instance
     */
    Addition.create = function create(properties) {
        return new Addition(properties);
    };

    /**
     * Encodes the specified Addition message. Does not implicitly {@link Addition.verify|verify} messages.
     * @function encode
     * @memberof Addition
     * @static
     * @param {IAddition} message Addition message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Addition.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.a != null && Object.hasOwnProperty.call(message, "a"))
            writer.uint32(/* id 1, wireType 0 =*/8).int32(message.a);
        if (message.b != null && Object.hasOwnProperty.call(message, "b"))
            writer.uint32(/* id 2, wireType 0 =*/16).int32(message.b);
        return writer;
    };

    /**
     * Encodes the specified Addition message, length delimited. Does not implicitly {@link Addition.verify|verify} messages.
     * @function encodeDelimited
     * @memberof Addition
     * @static
     * @param {IAddition} message Addition message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Addition.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes an Addition message from the specified reader or buffer.
     * @function decode
     * @memberof Addition
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {Addition} Addition
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Addition.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.Addition();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1: {
                    message.a = reader.int32();
                    break;
                }
            case 2: {
                    message.b = reader.int32();
                    break;
                }
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes an Addition message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof Addition
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {Addition} Addition
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Addition.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies an Addition message.
     * @function verify
     * @memberof Addition
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    Addition.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.a != null && message.hasOwnProperty("a"))
            if (!$util.isInteger(message.a))
                return "a: integer expected";
        if (message.b != null && message.hasOwnProperty("b"))
            if (!$util.isInteger(message.b))
                return "b: integer expected";
        return null;
    };

    /**
     * Creates an Addition message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof Addition
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {Addition} Addition
     */
    Addition.fromObject = function fromObject(object) {
        if (object instanceof $root.Addition)
            return object;
        var message = new $root.Addition();
        if (object.a != null)
            message.a = object.a | 0;
        if (object.b != null)
            message.b = object.b | 0;
        return message;
    };

    /**
     * Creates a plain object from an Addition message. Also converts values to other types if specified.
     * @function toObject
     * @memberof Addition
     * @static
     * @param {Addition} message Addition
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    Addition.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            object.a = 0;
            object.b = 0;
        }
        if (message.a != null && message.hasOwnProperty("a"))
            object.a = message.a;
        if (message.b != null && message.hasOwnProperty("b"))
            object.b = message.b;
        return object;
    };

    /**
     * Converts this Addition to JSON.
     * @function toJSON
     * @memberof Addition
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    Addition.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for Addition
     * @function getTypeUrl
     * @memberof Addition
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    Addition.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
        if (typeUrlPrefix === undefined) {
            typeUrlPrefix = "type.googleapis.com";
        }
        return typeUrlPrefix + "/Addition";
    };

    return Addition;
})();

module.exports = $root;
