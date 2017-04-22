/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * David Blasby (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.datastream.v2_3;


import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence.Float;

import java.util.List;

/**
 * Seralized form is a int[][]
 *    serializedForm[0] -- x coords
 *    serializedForm[1] -- y coords
 *
 *    serializedForm[0][0] - int representation of first X  (Float.intBitsToFloat)
 *    serializedForm[0][1] - 2nd X.  Float.intBitsToFloat(serializedForm[0][0] + serializedForm[0][1])
 *
 *    The ordinate list is a delta list on the Int32 form of the Float ordinate.
 *    This allows for exact representation as well as good VarInt encoding.
 */
public class FloatPackedCoordinateSequence extends Float {

    public static final CoordinateSequence EMPTY_2D = new FloatPackedCoordinateSequence(2, 0);

    public FloatPackedCoordinateSequence(final int dimensions, List<Coordinate> coords) {
       super(coords.toArray(new Coordinate[coords.size()]), dimensions);
    }
    public FloatPackedCoordinateSequence(final int dimensions, final int initialSize) {
        super(initialSize,dimensions);
    }
    public FloatPackedCoordinateSequence(int[][] serializedForm) {
        super(deserializeCoords(serializedForm),serializedForm.length);
    }

    public int[][] toSerializedForm() {
        int dims = this.getDimension();
        boolean hasZ = dims >2;
        int nCoords = size();
        int[] Xs = new int[nCoords];
        int[] Ys = new int[nCoords];
        int[] Zs = null;
        if (hasZ)
            Zs = new int[nCoords];

        int X=0;
        int Y=0;
        int Z=0;

        float[] allOrdinates = getRawCoordinates();

        for (int t=0;t<nCoords;t++) {
            int currentX = java.lang.Float.floatToRawIntBits(allOrdinates[t*dims] );
            int currentY = java.lang.Float.floatToRawIntBits(allOrdinates[t*dims +1] );
            Xs[t] = currentX - X;
            Ys[t] = currentY - Y;

            X = currentX;
            Y = currentY;

            if (hasZ) {
                int currentZ = java.lang.Float.floatToRawIntBits(allOrdinates[t*dims +1] );
                Zs[t] = currentZ - Z;
                Z = currentZ;
            }
        }
        if (hasZ) {
            return new int[][] {Xs,Ys,Zs};
        }
        return new int[][] {Xs,Ys};
    }

    private static float[] deserializeCoords(int[][] serializedForm) {
        int nCoords = serializedForm[0].length;
        int dims = serializedForm.length;
        boolean hasZ = dims >2;
        float[] result = new float[nCoords*2];
        if (nCoords ==0)
            return result; // empty

        int X =0;
        int Y=0;
        int Z=0;
        for (int t=0;t<nCoords;t++) {
            X += serializedForm[0][t];
            Y += serializedForm[1][t];
            result[t*dims] = java.lang.Float.intBitsToFloat(X);
            result[t*dims+1] = java.lang.Float.intBitsToFloat(Y);
            if (hasZ) {
                Z += serializedForm[2][t];
                result[t*dims+2] = java.lang.Float.intBitsToFloat(Y);
            }
        }
        return result;
    }

}
