package org.apache.poi.hslf.examples;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import org.apache.poi.hslf.record.InteractiveInfoAtom;
import org.apache.poi.hslf.record.RecordTypes;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSheet;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFSoundData;


public class SoundFinder {
	public static void main(String[] args) throws IOException {
		FileInputStream fis = new FileInputStream(args[0]);
		HSLFSlideShow ppt = new HSLFSlideShow(fis);
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
		ppt.close();
		fis.close();
	}

	protected static int getSoundReference(HSLFShape shape) {
		int soundRef = -1;
		InteractiveInfoAtom info = shape.getClientDataRecord(RecordTypes.InteractiveInfo.typeID);
		if ((info != null) && ((info.getAction()) == (InteractiveInfoAtom.ACTION_MEDIA))) {
			soundRef = info.getSoundRef();
		}
		return soundRef;
	}
}

