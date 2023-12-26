package org.apache.poi.hslf.examples;


import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.ddf.EscherClientDataRecord;
import org.apache.poi.ddf.EscherContainerRecord;
import org.apache.poi.ddf.EscherRecord;
import org.apache.poi.hslf.record.InteractiveInfo;
import org.apache.poi.hslf.record.InteractiveInfoAtom;
import org.apache.poi.hslf.record.Record;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSheet;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFSoundData;


public class SoundFinder {
	public static void main(String[] args) throws Exception {
		HSLFSlideShow ppt = new HSLFSlideShow(new FileInputStream(args[0]));
		HSLFSoundData[] sounds = ppt.getSoundData();
		for (HSLFSlide slide : ppt.getSlides()) {
			for (HSLFShape shape : slide.getShapes()) {
				int soundRef = SoundFinder.getSoundReference(shape);
				if (soundRef == (-1))
					continue;

				System.out.println(((((("Slide[" + (slide.getSlideNumber())) + "], shape[") + (shape.getShapeId())) + "], soundRef: ") + soundRef));
				System.out.println(("  " + (sounds[soundRef].getSoundName())));
				System.out.println(("  " + (sounds[soundRef].getSoundType())));
			}
		}
	}

	protected static int getSoundReference(HSLFShape shape) {
		int soundRef = -1;
		EscherContainerRecord spContainer = shape.getSpContainer();
		for (EscherRecord obj : spContainer.getChildRecords()) {
			if ((obj.getRecordId()) == (EscherClientDataRecord.RECORD_ID)) {
				byte[] data = obj.serialize();
				for (Record record : Record.findChildRecords(data, 8, ((data.length) - 8))) {
					if (record instanceof InteractiveInfo) {
						InteractiveInfoAtom info = ((InteractiveInfo) (record)).getInteractiveInfoAtom();
						if ((info.getAction()) == (InteractiveInfoAtom.ACTION_MEDIA)) {
							soundRef = info.getSoundRef();
						}
					}
				}
			}
		}
		return soundRef;
	}
}

