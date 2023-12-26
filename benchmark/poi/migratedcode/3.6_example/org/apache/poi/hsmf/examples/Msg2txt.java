package org.apache.poi.hsmf.examples;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.poi.hsmf.MAPIMessage;
import org.apache.poi.hsmf.datatypes.AttachmentChunks;
import org.apache.poi.hsmf.exceptions.ChunkNotFoundException;


public class Msg2txt {
	private String fileNameStem;

	private MAPIMessage msg;

	public Msg2txt(String fileName) throws IOException {
		fileNameStem = fileName;
		if ((fileNameStem.endsWith(".msg")) || (fileNameStem.endsWith(".MSG"))) {
			fileNameStem = fileNameStem.substring(0, ((fileNameStem.length()) - 4));
		}
		msg = new MAPIMessage(fileName);
	}

	public void processMessage() throws IOException {
		String txtFileName = (fileNameStem) + ".txt";
		String attDirName = (fileNameStem) + "-att";
		PrintWriter txtOut = null;
		try {
			txtOut = new PrintWriter(txtFileName);
			try {
				String displayFrom = msg.getDisplayFrom();
				txtOut.println(("From: " + displayFrom));
			} catch (ChunkNotFoundException e) {
			}
			try {
				String displayTo = msg.getDisplayTo();
				txtOut.println(("To: " + displayTo));
			} catch (ChunkNotFoundException e) {
			}
			try {
				String displayCC = msg.getDisplayCC();
				txtOut.println(("CC: " + displayCC));
			} catch (ChunkNotFoundException e) {
			}
			try {
				String displayBCC = msg.getDisplayBCC();
				txtOut.println(("BCC: " + displayBCC));
			} catch (ChunkNotFoundException e) {
			}
			try {
				String subject = msg.getSubject();
				txtOut.println(("Subject: " + subject));
			} catch (ChunkNotFoundException e) {
			}
			try {
				String body = msg.getTextBody();
				txtOut.println(body);
			} catch (ChunkNotFoundException e) {
				System.err.println("No message body");
			}
			Map attachmentMap = ((Map) (msg.getAttachmentFiles()));
			if ((attachmentMap.size()) > 0) {
				File d = new File(attDirName);
				if (d.mkdir()) {
					for (Iterator ii = attachmentMap.entrySet().iterator(); ii.hasNext();) {
						Map.Entry entry = ((Map.Entry) (ii.next()));
						processAttachment(d, entry.getKey().toString(), ((ByteArrayInputStream) (entry.getValue())));
					}
				}else {
					System.err.println(("Can't create directory " + attDirName));
				}
			}
		} finally {
			if (txtOut != null) {
				txtOut.close();
			}
		}
	}

	public void processAttachment(File dir, String fileName, ByteArrayInputStream fileIn) throws IOException {
		File f = new File(dir, fileName);
		OutputStream fileOut = null;
		try {
			fileOut = new FileOutputStream(f);
			byte[] buffer = new byte[2048];
			int bNum = fileIn.read(buffer);
			while (bNum > 0) {
				fileOut.write(buffer);
				bNum = fileIn.read(buffer);
			} 
		} finally {
			try {
				if (fileIn != null) {
					fileIn.close();
				}
			} finally {
				if (fileOut != null) {
					fileOut.close();
				}
			}
		}
	}

	public static void main(String[] args) {
		if ((args.length) <= 0) {
			System.err.println("No files names provided");
		}else {
			for (int i = 0; i < (args.length); i++) {
				try {
					Msg2txt processor = new Msg2txt(args[i]);
					processor.processMessage();
				} catch (IOException e) {
					System.err.println(((("Could not process " + (args[i])) + ": ") + e));
				}
			}
		}
	}
}

