package org.apache.poi.hslf.usermodel.examples;


import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import org.apache.poi.ddf.EscherClientDataRecord;
import org.apache.poi.ddf.EscherContainerRecord;
import org.apache.poi.ddf.EscherRecord;
import org.apache.poi.hslf.model.Shape;
import org.apache.poi.hslf.model.Sheet;
import org.apache.poi.hslf.model.Slide;
import org.apache.poi.hslf.record.InteractiveInfo;
import org.apache.poi.hslf.record.InteractiveInfoAtom;
import org.apache.poi.hslf.record.Record;
import org.apache.poi.hslf.usermodel.SlideShow;
import org.apache.poi.hslf.usermodel.SoundData;


public class SoundFinder {
	public static void main(String[] args) throws Exception {
		SlideShow ppt = new SlideShow(new FileInputStream(args[0]));
		SoundData[] sounds = ppt.getSoundData();
		Slide[] slide = ppt.getSlides();
		for (int i = 0; i < (slide.length); i++) {
			Shape[] shape = slide[i].getShapes();
			for (int j = 0; j < (shape.length); j++) {
				int soundRef = SoundFinder.getSoundReference(shape[j]);
				if (soundRef != (-1)) {
					System.out.println(((((("Slide[" + i) + "], shape[") + j) + "], soundRef: ") + soundRef));
					System.out.println(("  " + (sounds[soundRef].getSoundName())));
					System.out.println(("  " + (sounds[soundRef].getSoundType())));
				}
			}
		}
	}

	protected static int getSoundReference(Shape shape) {
		int soundRef = -1;
		EscherContainerRecord spContainer = shape.getSpContainer();
		List spchild = spContainer.getChildRecords();
		for (Iterator it = spchild.iterator(); it.hasNext();) {
			EscherRecord obj = ((EscherRecord) (it.next()));
			if ((obj.getRecordId()) == (EscherClientDataRecord.RECORD_ID)) {
				byte[] data = obj.serialize();
				Record[] records = Record.findChildRecords(data, 8, ((data.length) - 8));
				for (int j = 0; j < (records.length); j++) {
					if ((records[j]) instanceof InteractiveInfo) {
						InteractiveInfoAtom info = ((InteractiveInfo) (records[j])).getInteractiveInfoAtom();
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

