FOR /L %%A IN (1,1,10) DO (
  ECHO Calling Order Generator#  %%A
  java -jar order-generator.jar
)