package com.pinternals.diffo;

public class UUtil {
	private static byte h2b(byte a) {
		int i = 0;
		if (a>=97)
			i = a - 87;
		else if (a>=65)
			i = a - 55;
		else if (a>=48)
			i = a - 48;
		if (i<0 || i>15)
			throw new RuntimeException("invalid input " + a + " for hex2byte is given");
		return (byte)i;
	}

	private static int bo[] = new int[] {0,0,0,0, 1,1, 2,2, 3,3, 4,4,4,4,4,4,4,4,4};
	public static byte[] getBytesUUIDfromString(String r) {
		// given string is either 'xxxxxxxxxxxxxxxxxxxxxxxxxxx' or 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
		byte n[] = new byte[16];
		byte s[] = r.getBytes();
		if (s[8]=='-') {
			for (int i=0;i<16;i++) 
				n[i] = (byte) (h2b(s[i*2+bo[i]])*16 + h2b(s[i*2+1+bo[i]]));
		} else {
			for (int i=0;i<16;i++)
				n[i] = (byte) (h2b(s[i*2])*16 + h2b(s[i*2+1]));
		}
		return n;
	}
	public static boolean areEquals(byte[] g1, byte[] g2) {
		if ((g1==null && g2==null) 
			|| (g1==null && g2.length == 0) 
			|| (g2==null && g1.length == 0) )
			return true;
		boolean b = true;
		for (int i=0; i<g1.length; i++)
			b = b && g1[i]==g2[i];
		return b;
	}
	
	private static char co[] = new char[] {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
	public static String getStringUUIDfromBytes(byte a[]) {
		return getStringUUIDfromBytes(a,false);
	}
	public static String getStringUUIDfromBytes(byte a[], boolean minus) {
		if (a==null) return null;
		assert a.length==16 : "UUID has 16 bytes, only " + a.length + " is given";
		char c[] = null;
		if (!minus) { 
			c = new char[32];
			for (int i=0; i<16; i++) {
				int j = a[i] & 0xff;
				c[i*2] = co[(j/16)];
				c[i*2+1] = co[j%16];
//				System.out.println("given:"+j+", =>" + c[i*2] + c[i*2+1]);
			}
		} else {
			c = new char[36];
			c[8] = c[13] = c[18] = c[23] = '-';
			for (int i=0; i<16; i++) {
				int j = a[i] & 0xff;
				c[i*2 + bo[i]] = co[(j/16)];
				c[i*2+1 + bo[i]] = co[j%16];
			}
		}
		return new String(c);
	}
	
	public static byte[] getBytesUUIDfromLongPair(long most, long least) {
		// taken from stackoverflow.com
		byte b[] = new byte[16];
		int i=0;
		b[i++] = (byte) (most >> 56);
		b[i++] = (byte) (most >> 48);
		b[i++] = (byte) (most >> 40);
		b[i++] = (byte) (most >> 32);
		b[i++] = (byte) (most >> 24);
		b[i++] = (byte) (most >> 16);
		b[i++] = (byte) (most >> 8);
		b[i++] = (byte) (most);
		b[i++] = (byte) (least >> 56);
		b[i++] = (byte) (least >> 48);
		b[i++] = (byte) (least >> 40);
		b[i++] = (byte) (least >> 32);
		b[i++] = (byte) (least >> 24);
		b[i++] = (byte) (least >> 16);
		b[i++] = (byte) (least >> 8);
		b[i++] = (byte) (least);
		return b;
	}
	
	private static long getLongBA(byte b[], int off) {
		// taken from stackoverflow.com
	    return ((b[off + 7] & 0xFFL) << 0) +
	           ((b[off + 6] & 0xFFL) << 8) +
	           ((b[off + 5] & 0xFFL) << 16) +
	           ((b[off + 4] & 0xFFL) << 24) +
	           ((b[off + 3] & 0xFFL) << 32) +
	           ((b[off + 2] & 0xFFL) << 40) +
	           ((b[off + 1] & 0xFFL) << 48) +
	           (((long) b[off + 0]) << 56);
	}
	
	public static long[] getLongPairFromBytesUUID(byte[] b) {
		// order is: most, least
		long l[] = new long[2];
		l[0] = getLongBA(b, 0); // most
		l[1] = getLongBA(b, 8); // least
		return l;
	}

	public static String getStringUUIDfromLongPair(long most, long least, boolean minus) {
		return getStringUUIDfromBytes(getBytesUUIDfromLongPair(most, least), minus);
	}
	
	public static boolean assertion() {
		long[] a;
		byte ba[];
		String s1 = "000102030405060708090A0B0C0D0E0F"
			, s2 = "00010203-0405-0607-0809-0A0b0c0d0e0f"
			, s3 = "102030405060708090A0B0C0D0E0F0FF"
			, s4 = "abcdef0123456789ABCDE01234567890"
			;
		ba = getBytesUUIDfromString(s1);
		assert s1.equalsIgnoreCase(getStringUUIDfromBytes(ba)) : "UUID logic";
		assert s1.equalsIgnoreCase(getStringUUIDfromBytes(ba,false)) : "UUID logic";
		assert s2.equalsIgnoreCase(getStringUUIDfromBytes(ba,true)) : "UUID logic";
		assert s3.equalsIgnoreCase( getStringUUIDfromBytes( getBytesUUIDfromString(s3) )) : "UUID logic";
		a = getLongPairFromBytesUUID( getBytesUUIDfromString(s4) );
		assert s4.equalsIgnoreCase( getStringUUIDfromLongPair(a[0], a[1], false) );
		
		assert areEquals(getBytesUUIDfromLongPair(123, 456), getBytesUUIDfromLongPair(123, 456));
		assert areEquals(null, null);
		assert areEquals(new byte[0], new byte[0]);
		assert areEquals(ba,ba);

		return true;
	}
}
