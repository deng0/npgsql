#region License
// The PostgreSQL License
//
// Copyright (C) 2017 The Npgsql Development Team
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written
// agreement is hereby granted, provided that the above copyright notice
// and this paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE NPGSQL DEVELOPMENT TEAM BE LIABLE TO ANY PARTY
// FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
// INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE NPGSQL DEVELOPMENT TEAM HAS BEEN ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
//
// THE NPGSQL DEVELOPMENT TEAM SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE NPGSQL DEVELOPMENT TEAM HAS NO OBLIGATIONS
// TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#endregion

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.Logging;
using Npgsql.PostgresTypes;
using NpgsqlTypes;

namespace Npgsql.TypeHandlers
{
    /// <summary>
    /// Global PostgisConfig.
    /// </summary>
    public static class PostgisConfig
    {
        /// <summary>
        /// Whether PostGIS geometries should be parsed.
        /// </summary>
        public static bool ParseGeometries = true;
    }

    /// <summary>
    /// Type Handler for the postgis geometry type.
    /// </summary>
    [TypeMapping("geometry", NpgsqlDbType.Geometry, new[]
    {
        typeof(PostgisGeometry),
        typeof(PostgisRawGeometry),
        typeof(PostgisPoint),
        typeof(PostgisMultiPoint),
        typeof(PostgisLineString),
        typeof(PostgisMultiLineString),
        typeof(PostgisPolygon),
        typeof(PostgisMultiPolygon),
        typeof(PostgisGeometryCollection),
    })]
    class PostgisGeometryHandler : ChunkingTypeHandler<PostgisGeometry>,
        IChunkingTypeHandler<PostgisRawGeometry>,
        IChunkingTypeHandler<PostgisPoint>, IChunkingTypeHandler<PostgisMultiPoint>,
        IChunkingTypeHandler<PostgisLineString>, IChunkingTypeHandler<PostgisMultiLineString>,
        IChunkingTypeHandler<PostgisPolygon>, IChunkingTypeHandler<PostgisMultiPolygon>,
        IChunkingTypeHandler<PostgisGeometryCollection>,
        IChunkingTypeHandler<byte[]>
    {
        [CanBeNull]
        internal ByteaHandler ByteaHandler { get; }

        static readonly NpgsqlLogger Log = NpgsqlLogManager.GetCurrentClassLogger();

        internal PostgisGeometryHandler(PostgresType postgresType, TypeHandlerRegistry registry)
            : base(postgresType)
        {
            var byteaHandler = registry[NpgsqlDbType.Bytea];
            if (byteaHandler == registry.UnrecognizedTypeHandler)
            {
                Log.Warn("bytea type not present when setting up postgis geometry type. Writing as bytea will not work.");
                return;
            }
            ByteaHandler = (ByteaHandler)byteaHandler;
        }

        #region Read

        public override async ValueTask<PostgisGeometry> Read(ReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            return await Read(buf, len, async, fieldDescription, PostgisConfig.ParseGeometries);
        }

        async ValueTask<PostgisGeometry> Read(ReadBuffer buf, int len, bool async, FieldDescription fieldDescription, bool parseGeometry)
        {
            await buf.Ensure(5, async);
            var bo = (ByteOrder)buf.ReadByte();
            var id = buf.ReadUInt32(bo);

            var srid = 0u;
            if ((id & (uint)EwkbModifiers.HasSRID) != 0)
            {
                await buf.Ensure(4, async);
                srid = buf.ReadUInt32(bo);
                id &= ~(uint)EwkbModifiers.HasSRID;
            }

            if (!parseGeometry)
            {
                byte[] bytes = new byte[srid != 0 ? len - 4 : len];
                bytes[0] = (byte)bo;

                if (bo == ByteOrder.LSB)
                {
                    bytes[1] = (byte)id;
                    bytes[2] = (byte)(id >> 8);
                    bytes[3] = (byte)(id >> 16);
                    bytes[4] = (byte)(id >> 24);
                }
                else
                {
                    bytes[1] = (byte)(id >> 24);
                    bytes[2] = (byte)(id >> 16);
                    bytes[3] = (byte)(id >> 8);
                    bytes[4] = (byte)id;
                }

                var wkbPos = 5;
                var toRead = Math.Min(bytes.Length - wkbPos, buf.Size);
                while (toRead > 0)
                {
                    await buf.Ensure(toRead, async);
                    buf.ReadBytes(bytes, wkbPos, toRead);
                    wkbPos += toRead;
                    toRead = Math.Min(bytes.Length - wkbPos, buf.Size);
                }

                return new PostgisRawGeometry(srid, bytes);
            }

            var geom = await DoRead(buf, (WkbIdentifier)(id & 7), bo, async);
            geom.SRID = srid;
            return geom;
        }

        async ValueTask<PostgisGeometry> DoRead(ReadBuffer buf, WkbIdentifier id, ByteOrder bo, bool async)
        {
            switch (id)
            {
            case WkbIdentifier.Point:
                await buf.Ensure(16, async);
                return new PostgisPoint(buf.ReadDouble(bo), buf.ReadDouble(bo));

            case WkbIdentifier.LineString:
            {
                await buf.Ensure(4, async);
                var points = new Coordinate2D[buf.ReadInt32(bo)];
                for (var ipts = 0; ipts < points.Length; ipts++)
                {
                    await buf.Ensure(16, async);
                    points[ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                }
                return new PostgisLineString(points);
            }

            case WkbIdentifier.Polygon:
            {
                await buf.Ensure(4, async);
                var rings = new Coordinate2D[buf.ReadInt32(bo)][];

                for (var irng = 0; irng < rings.Length; irng++)
                {
                    await buf.Ensure(4, async);
                    rings[irng] = new Coordinate2D[buf.ReadInt32(bo)];
                    for (var ipts = 0; ipts < rings[irng].Length; ipts++)
                    {
                        await buf.Ensure(16, async);
                        rings[irng][ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                    }
                }
                return new PostgisPolygon(rings);
            }

            case WkbIdentifier.MultiPoint:
            {
                await buf.Ensure(4, async);
                var points = new Coordinate2D[buf.ReadInt32(bo)];
                for (var ipts = 0; ipts < points.Length; ipts++)
                {
                    await buf.Ensure(21, async);
                    await buf.Skip(5, async);
                    points[ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                }
                return new PostgisMultiPoint(points);
            }

            case WkbIdentifier.MultiLineString:
            {
                await buf.Ensure(4, async);
                var rings = new Coordinate2D[buf.ReadInt32(bo)][];

                for (var irng = 0; irng < rings.Length; irng++)
                {
                    await buf.Ensure(9, async);
                    await buf.Skip(5, async);
                    rings[irng] = new Coordinate2D[buf.ReadInt32(bo)];
                    for (var ipts = 0; ipts < rings[irng].Length; ipts++)
                    {
                        await buf.Ensure(16, async);
                        rings[irng][ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                    }
                }
                return new PostgisMultiLineString(rings);
            }

            case WkbIdentifier.MultiPolygon:
            {
                await buf.Ensure(4, async);
                var pols = new Coordinate2D[buf.ReadInt32(bo)][][];

                for (var ipol = 0; ipol < pols.Length; ipol++)
                {
                    await buf.Ensure(9, async);
                    await buf.Skip(5, async);
                    pols[ipol] = new Coordinate2D[buf.ReadInt32(bo)][];
                    for (var irng = 0; irng < pols[ipol].Length; irng++)
                    {
                        await buf.Ensure(4, async);
                        pols[ipol][irng] = new Coordinate2D[buf.ReadInt32(bo)];
                        for (var ipts = 0; ipts < pols[ipol][irng].Length; ipts++)
                        {
                            await buf.Ensure(16, async);
                            pols[ipol][irng][ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                        }
                    }
                }
                return new PostgisMultiPolygon(pols);
            }

            case WkbIdentifier.GeometryCollection:
            {
                await buf.Ensure(4, async);
                var g = new PostgisGeometry[buf.ReadInt32(bo)];

                for (var i = 0; i < g.Length; i++)
                {
                    await buf.Ensure(5, async);
                    var elemBo = (ByteOrder)buf.ReadByte();
                    var elemId = (WkbIdentifier)(buf.ReadUInt32(bo) & 7);

                    g[i] = await DoRead(buf, elemId, elemBo, async);
                }
                return new PostgisGeometryCollection(g);
            }

            default:
                throw new InvalidOperationException("Unknown Postgis identifier.");
            }
        }

        ValueTask<byte[]> IChunkingTypeHandler<byte[]>.Read(ReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
        {
            Debug.Assert(ByteaHandler != null);
            return ByteaHandler.Read(buf, len, async, fieldDescription);
        }

        #endregion Read

        #region Read concrete types
        async ValueTask<PostgisRawGeometry> IChunkingTypeHandler<PostgisRawGeometry>.Read(ReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisRawGeometry)await Read(buf, len, async, fieldDescription, false);
        async ValueTask<PostgisPoint> IChunkingTypeHandler<PostgisPoint>.Read(ReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisPoint)await Read(buf, len, async, fieldDescription, true);
        async ValueTask<PostgisMultiPoint> IChunkingTypeHandler<PostgisMultiPoint>.Read(ReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisMultiPoint)await Read(buf, len, async, fieldDescription, true);
        async ValueTask<PostgisLineString> IChunkingTypeHandler<PostgisLineString>.Read(ReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisLineString)await Read(buf, len, async, fieldDescription, true);
        async ValueTask<PostgisMultiLineString> IChunkingTypeHandler<PostgisMultiLineString>.Read(ReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisMultiLineString)await Read(buf, len, async, fieldDescription, true);
        async ValueTask<PostgisPolygon> IChunkingTypeHandler<PostgisPolygon>.Read(ReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisPolygon)await Read(buf, len, async, fieldDescription, true);
        async ValueTask<PostgisMultiPolygon> IChunkingTypeHandler<PostgisMultiPolygon>.Read(ReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisMultiPolygon)await Read(buf, len, async, fieldDescription, true);
        async ValueTask<PostgisGeometryCollection> IChunkingTypeHandler<PostgisGeometryCollection>.Read(ReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => (PostgisGeometryCollection)await Read(buf, len, async, fieldDescription, true);

        #endregion

        #region Write

        public override int ValidateAndGetLength(object value, ref LengthCache lengthCache, NpgsqlParameter parameter = null)
        {
            var asGeometry = value as PostgisGeometry;
            if (asGeometry != null)
                return asGeometry.GetLen(true);

            var asBytes = value as byte[];
            if (asBytes != null)
                return asBytes.Length;

            throw new InvalidCastException("IGeometry type expected.");
        }

        protected override Task Write(object value, WriteBuffer buf, LengthCache lengthCache, NpgsqlParameter parameter,
            bool async, CancellationToken cancellationToken)
        {
            var bytes = value as byte[];
            if (bytes != null)
            {
                if (ByteaHandler == null)
                    throw new NpgsqlException("Bytea handler was not found during initialization of PostGIS handler");
                return ByteaHandler.WriteInternal(bytes, buf, lengthCache, parameter, async, cancellationToken);
            }

            return Write((PostgisGeometry)value, buf, lengthCache, parameter, async, cancellationToken);
        }

        static uint AdjustByteOrder(uint x, bool swap)
        {
            return swap ? ((x & 0x000000FF) << 24) | ((x & 0x0000FF00) << 8) | ((x & 0x00FF0000) >> 8) | ((x & 0xFF000000) >> 24) : x;
        }

        async Task Write(PostgisGeometry geom, WriteBuffer buf, LengthCache lengthCache, NpgsqlParameter parameter,
            bool async, CancellationToken cancellationToken)
        {
            // Common header
            if (geom.SRID == 0)
            {
                if (buf.WriteSpaceLeft < 5)
                    await buf.Flush(async, cancellationToken);
                ByteOrder byteOrder = geom.ByteOrder;
                buf.WriteByte((byte)byteOrder);
                buf.WriteUInt32(AdjustByteOrder(geom.WkbType, byteOrder == ByteOrder.LSB));
            }
            else
            {
                if (buf.WriteSpaceLeft < 9)
                    await buf.Flush(async, cancellationToken);
                ByteOrder byteOrder = geom.ByteOrder;
                buf.WriteByte((byte)byteOrder);
                buf.WriteUInt32(AdjustByteOrder(geom.WkbType | (uint)EwkbModifiers.HasSRID, byteOrder == ByteOrder.LSB));
                buf.WriteUInt32(AdjustByteOrder(geom.SRID, byteOrder == ByteOrder.LSB));
            }

            switch (geom.Identifier)
            {
            case WkbIdentifier.Raw:
                var raw = (PostgisRawGeometry)geom;
                var wkbPos = 5;
                while (true)
                {
                    var bytesToWrite = Math.Min(raw.Wkb.Length - wkbPos, buf.WriteSpaceLeft);
                    if (bytesToWrite > 0)
                    {
                        buf.WriteBytes(raw.Wkb, wkbPos, bytesToWrite);
                        wkbPos += bytesToWrite;
                    }
                    if (wkbPos == raw.Wkb.Length)
                        break;

                    await buf.Flush(async, cancellationToken);
                }
                return;

            case WkbIdentifier.Point:
                if (buf.WriteSpaceLeft < 16)
                    await buf.Flush(async, cancellationToken);
                var p = (PostgisPoint)geom;
                buf.WriteDouble(p.X);
                buf.WriteDouble(p.Y);
                return;

            case WkbIdentifier.LineString:
                var l = (PostgisLineString)geom;
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async, cancellationToken);
                buf.WriteInt32(l.PointCount);
                for (var ipts = 0; ipts < l.PointCount; ipts++)
                {
                    if (buf.WriteSpaceLeft < 16)
                        await buf.Flush(async, cancellationToken);
                    buf.WriteDouble(l[ipts].X);
                    buf.WriteDouble(l[ipts].Y);
                }
                return;

            case WkbIdentifier.Polygon:
                var pol = (PostgisPolygon)geom;
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async, cancellationToken);
                buf.WriteInt32(pol.RingCount);
                for (var irng = 0; irng < pol.RingCount; irng++)
                {
                    if (buf.WriteSpaceLeft < 4)
                        await buf.Flush(async, cancellationToken);
                    buf.WriteInt32(pol[irng].Length);
                    for (var ipts = 0; ipts < pol[irng].Length; ipts++)
                    {
                        if (buf.WriteSpaceLeft < 16)
                            await buf.Flush(async, cancellationToken);
                        buf.WriteDouble(pol[irng][ipts].X);
                        buf.WriteDouble(pol[irng][ipts].Y);
                    }
                }
                return;

            case WkbIdentifier.MultiPoint:
                var mp = (PostgisMultiPoint)geom;
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async, cancellationToken);
                buf.WriteInt32(mp.PointCount);
                for (var ipts = 0; ipts < mp.PointCount; ipts++)
                {
                    if (buf.WriteSpaceLeft < 21)
                        await buf.Flush(async, cancellationToken);
                    buf.WriteByte(0);
                    buf.WriteInt32((int)WkbIdentifier.Point);
                    buf.WriteDouble(mp[ipts].X);
                    buf.WriteDouble(mp[ipts].Y);
                }
                return;

            case WkbIdentifier.MultiLineString:
                var ml = (PostgisMultiLineString)geom;
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async, cancellationToken);
                buf.WriteInt32(ml.LineCount);
                for (var irng = 0; irng < ml.LineCount; irng++)
                {
                    if (buf.WriteSpaceLeft < 9)
                        await buf.Flush(async, cancellationToken);
                    buf.WriteByte(0);
                    buf.WriteInt32((int)WkbIdentifier.LineString);
                    buf.WriteInt32(ml[irng].PointCount);
                    for (var ipts = 0; ipts < ml[irng].PointCount; ipts++)
                    {
                        if (buf.WriteSpaceLeft < 16)
                            await buf.Flush(async, cancellationToken);
                        buf.WriteDouble(ml[irng][ipts].X);
                        buf.WriteDouble(ml[irng][ipts].Y);
                    }
                }
                return;

            case WkbIdentifier.MultiPolygon:
                var mpl = (PostgisMultiPolygon)geom;
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async, cancellationToken);
                buf.WriteInt32(mpl.PolygonCount);
                for (var ipol = 0; ipol < mpl.PolygonCount; ipol++)
                {
                    if (buf.WriteSpaceLeft < 9)
                        await buf.Flush(async, cancellationToken);
                    buf.WriteByte(0);
                    buf.WriteInt32((int)WkbIdentifier.Polygon);
                    buf.WriteInt32(mpl[ipol].RingCount);
                    for (var irng = 0; irng < mpl[ipol].RingCount; irng++)
                    {
                        if (buf.WriteSpaceLeft < 4)
                            await buf.Flush(async, cancellationToken);
                        buf.WriteInt32(mpl[ipol][irng].Length);
                        for (var ipts = 0; ipts < mpl[ipol][irng].Length; ipts++)
                        {
                            if (buf.WriteSpaceLeft < 16)
                                await buf.Flush(async, cancellationToken);
                            buf.WriteDouble(mpl[ipol][irng][ipts].X);
                            buf.WriteDouble(mpl[ipol][irng][ipts].Y);
                        }
                    }
                }
                return;

            case WkbIdentifier.GeometryCollection:
                var coll = (PostgisGeometryCollection)geom;
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async, cancellationToken);
                buf.WriteInt32(coll.GeometryCount);

                foreach (var x in coll)
                    await Write(x, buf, lengthCache, null, async, cancellationToken);
                return;

            default:
                throw new InvalidOperationException("Unknown Postgis identifier.");
            }
        }

        #endregion Write
    }
}
