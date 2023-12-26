package org.apache.poi.hsmf.examples;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import org.apache.poi.hsmf.MAPIMessage;
import org.apache.poi.hsmf.datatypes.AttachmentChunks;
import org.apache.poi.hsmf.datatypes.ByteChunk;
import org.apache.poi.hsmf.datatypes.StringChunk;
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
			AttachmentChunks[] attachments = msg.getAttachmentFiles();
			if ((attachments.length) > 0) {
				File d = new File(attDirName);
				if (d.mkdir()) {
					for (AttachmentChunks attachment : attachments) {
						processAttachment(attachment, d);
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

	public void processAttachment(AttachmentChunks attachment, File dir) throws IOException {
		String fileName = attachment.getAttachFileName().toString();
		if ((attachment.getAttachLongFileName()) != null) {
			fileName = attachment.getAttachLongFileName().toString();
		}
		File f = new File(dir, fileName);
		OutputStream fileOut = null;
		try {
			fileOut = new FileOutputStream(f);
			fileOut.write(attachment.getAttachData().getValue());
		} finally {
			if (fileOut != null) {
				fileOut.close();
			}
		}
	}

	public static void main(String[] args) {
		if ((args.length) <= 0) {
			System.err.println("No files names provided");
		}else {
			for (String arg : args) {
				try {
					Msg2txt processor = new Msg2txt(arg);
					processor.processMessage();
				} catch (IOException e) {
					System.err.println(((("Could not process " + arg) + ": ") + e));
				}
			}
		}
	}
}

